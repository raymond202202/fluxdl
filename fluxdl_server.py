#!/usr/bin/env python3
"""
FluxDL 面板服务 — M2
单文件、零依赖（标准库 http.server），浅色极简 UI，SSE 实时进度。

安全（重要）：下载器天然是 RCE 级攻击面（它会把 URL 内容写进磁盘），所以：
  1. 只监听 127.0.0.1
  2. 所有 /api/* 都要 token（启动时生成，写进控制台与页面）
  3. 只接受 http/https 链接；每任务独立进程级线程池
"""
from __future__ import annotations

import hmac
import json
import os
import secrets
import signal
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import fluxdl

HOST = "127.0.0.1"
PORT = int(os.environ.get("FLUXDL_PORT", "8730"))
TOKEN = os.environ.get("FLUXDL_TOKEN") or secrets.token_urlsafe(16)
DOWNLOAD_DIR = os.environ.get("FLUXDL_DIR") or os.path.expanduser("~/Downloads/FluxDL")


# ---------------------------------------------------------------- P1-A 安全基线
# token 唯一的用途是挡住"浏览器里的其他站点"打本机 127.0.0.1 的 API
# （CSRF / DNS rebinding）。Host 头白名单是 rebinding 的标准挡法：
# 攻击者页面把 evil.com 解析到 127.0.0.1 时，浏览器发出的 Host 仍是 evil.com:PORT。
ALLOWED_HOSTS = {f"127.0.0.1:{PORT}", f"localhost:{PORT}", f"[::1]:{PORT}"}
_HOME = os.path.expanduser("~")

# SSE 一次性 nonce：EventSource 不能自定义请求头，token 只能进 URL；
# 但把【长期 token】放进 URL 太脏（历史记录/日志），所以换成短期一次性凭证。
NONCE_TTL = 600          # 10 分钟足够打开一个标签页
NONCE_MAX = 64
_NONCES: dict[str, float] = {}
_NONCES_LOCK = threading.Lock()

# P2-C：SSE 客户端上限。每个客户端 = 一个常驻线程 + 每 0.7s 一次序列化，
# 不限量的话"开一堆标签页"就能把 GIL 和带宽吃光。超限直接 503。
SSE_MAX = 4
_SSE_ACTIVE = 0
_SSE_LOCK = threading.Lock()
SSE_INTERVAL = 0.7

# P2-B：全局连接预算。MAX_ACTIVE 的注释写的是"避免把带宽切碎"，
# 但 /api/add 允许 concurrency 到 64 → 3×64=192 条连接，正好是最碎的用法。
# 改成"全局最多 8 个任务 × 每个任务上限 16"这种乘法式的显式预算：
# 同时下载任务数与单任务并发相乘不超过 CONN_BUDGET 条 TCP 连接。
# 16 的取值不是拍脑袋：内核 max_concurrency 硬上限就是 16
# （fluxdl.py:290 `min(int(max_concurrency), 16)`），传 >16 也只是被静默截断，
# 反而让用户以为"我设了 64"。降到 16 后面板接受的参数与内核实际行为一致，
# 2 个任务 × 16 = 32 条连接，是家用带宽上一个经得起实测的数。
CONC_MAX = 16
CONN_BUDGET = 32
SHUTDOWN_GRACE = 8.0   # P3-2：关机等内核收尾落盘的上限（秒）


def _new_nonce() -> str:
    now = time.time()
    with _NONCES_LOCK:
        for k, exp in list(_NONCES.items()):
            if exp < now:
                _NONCES.pop(k, None)
        if len(_NONCES) > NONCE_MAX:                 # 防御性：极端情况下清空重建
            _NONCES.clear()
        n = secrets.token_urlsafe(16)
        _NONCES[n] = now + NONCE_TTL
        return n


def _burn_nonce(n: str) -> bool:
    """校验并作废（一次性）。"""
    with _NONCES_LOCK:
        exp = _NONCES.pop(n, None)
    return bool(exp) and exp >= time.time()


class Task:
    _seq = 0
    _synced = True      # P1-B：是否已进过 SSE 版本号（首次推送时强制带全量字段）

    def __init__(self, url, *, referer=None, cookie=None, name=None, concurrency=0,
                 force_restart=False):
        # 注意：id 分配必须在 TASKS_LOCK 内完成（见 add_task），否则并发 add 会撞 id
        Task._seq += 1
        self.id = Task._seq
        self.url, self.referer, self.cookie, self.name = url, referer, cookie, name
        if not self.name:
            # P2-A：探测出文件名之前 out_path 是空串，to_dict() 的 basename 也为空
            # → 卡片标题空白，用户看不出在下什么。先拿 URL 的 basename 顶上。
            p = os.path.basename(urllib.parse.urlparse(url).path or "")
            self.name = urllib.parse.unquote(p) or url
        self.requested = concurrency
        # P0-1：面板侧也要能丢弃作废的检查点（源已变更时唯一的安全出路）。
        self.force_restart = bool(force_restart)
        self.state = "排队"          # 排队/下载中/完成/失败/已取消
        self.dl: fluxdl.Downloader | None = None
        # P3-2：关机时要 join 这个线程，等内核把 .fluxdl 收尾落盘再退
        self.thread: threading.Thread | None = None
        self.snapshot = {"percent": 0.0, "speed": 0, "eta": None, "done": 0,
                         "total": 0, "concurrency": 0}
        self.message = ""
        self.out_path = ""
        self.cancelled_before_start = False   # P1-2：线程启动前就被取消
        self.created = time.time()
        self._lock = threading.Lock()

    def start(self):
        self.state = "下载中"
        t = threading.Thread(target=self._run, daemon=True, name=f"fluxdl-task-{self.id}")
        self.thread = t
        t.start()
        return t

    def _run(self):
        # P1-1：Downloader(...) 的构造必须放在 try 内。
        # 原先构造在 try 之外，一旦构造抛异常（URL 非法、目录不可写、参数校验失败），
        # 线程直接死掉，state 永久停在"下载中"，MAX_ACTIVE 计数只增不减 ——
        # 累计 3 次之后面板再也起不了新任务，只能重启。
        try:
            # P1-2：cancel() 发生在"置下载中"之后、"线程真正跑起来"之前的窗口期里，
            # 原先线程照样建 Downloader 并 run() 到底，结束时又把 state 覆盖成"完成" ——
            # 用户点了取消，文件却继续下完并显示成功。
            if self.cancelled_before_start or self.state == "已取消":
                self.state = "已取消"
                return

            self.dl = fluxdl.Downloader(
                self.url, DOWNLOAD_DIR, concurrency=self.requested,
                referer=self.referer, cookie=self.cookie, override_name=self.name,
                on_progress=self._on_prog, force_restart=self.force_restart)

            res = self.dl.run()
            self.out_path = res["path"]
            # P0-1：面板必须像 CLI 一样做内容校验再报"完成"。
            # 见 BUG修复记录.md —— 验收标准是内容校验通过，不是文件存在且大小对。
            ok, info = fluxdl.verify_file(res["path"])
            if not ok:
                self.state = "失败"
                self.message = f"内容校验未通过: {info}"
            elif self.cancelled_before_start or self.state == "已取消":
                # 下载过程中被取消：run() 可能已经落盘，但状态不能被"完成"覆盖
                self.state = "已取消"
            else:
                self.state = "完成"
                self.message = str(info or "")
                # 小文件几十毫秒就下完，_on_prog 的 0.25s 节流可能一次都没触发，
                # 于是 snapshot 里的 done/total 停在 0，前端就显示 "0 B / 未知"
                # 而进度条是 100% —— 自相矛盾。完成时用 run() 的真实结果补齐。
                size = res.get("size") or 0
                with self._lock:
                    self.snapshot.update(
                        percent=100.0, speed=0, eta=0,
                        done=size, total=size or self.snapshot.get("total", 0))
        except fluxdl.DownloadError as e:
            if self.cancelled_before_start or self.state == "已取消":
                self.state = "已取消"
            else:
                self.state = "失败"
                self.message = str(e)
        except Exception as e:                       # 兜底，别让线程静默死掉
            self.state = "失败"
            self.message = f"{type(e).__name__}: {e}"
        finally:
            # P0-2：_pick_queued 原先只在 add_task 里调用一次，任务结束后无人推进队列
            _pick_queued()

    def _on_prog(self, p):
        with self._lock:
            self.snapshot.update(
                percent=round(p.get("percent", 0), 2),
                speed=p.get("speed", 0), eta=p.get("eta"),
                done=p.get("done", 0), total=p.get("total", 0),
                concurrency=p.get("concurrency", 0))

    def cancel(self):
        # P1-2：dl 还没建出来时也要留下"取消过"的痕迹，
        # 否则线程启动后无从得知自己被取消过。
        if self.dl:
            self.dl.stop()
        else:
            self.cancelled_before_start = True
        if self.state in ("排队", "下载中"):
            self.state = "已取消"

    def to_dict(self):
        with self._lock:
            snap = dict(self.snapshot)
        return {"id": self.id, "url": self.url, "state": self.state,
                # P2-A：按 out_path → 面板指定名 → URL basename 依次回退
                "name": (os.path.basename(self.out_path) or self.name
                         or os.path.basename(urllib.parse.urlparse(self.url).path)
                         or self.url),
                "path": self.out_path, "message": self.message,
                "concurrency": self.requested, **snap}

    def delta(self, prev):
        """P1-B：只返回与 prev 不同的字段（首次同步 prev 为 None → 全量）。"""
        cur = self.to_dict()
        if prev is None:
            return cur
        d = {"id": self.id}
        for k, v in cur.items():
            if k != "id" and prev.get(k) != v:
                d[k] = v
        return d


TASKS: dict[int, Task] = {}
TASKS_LOCK = threading.Lock()
MAX_ACTIVE = 3          # 同时下载任务数，避免把带宽切碎
HISTORY_KEEP = 200      # 已完成/失败/取消的任务最多保留这么多条


def _prune_tasks():
    """P2-4：TASKS 原先只增不减。

    面板跑一整天，几百个任务全留在字典里：每次 /api/list（前端 800ms 一次）
    都要把全部任务 to_dict() 一遍并序列化，前端 render() 也要遍历一遍 ——
    O(历史任务数) 的活儿每 0.8 秒干一次，越用越卡，且内存只涨不落。
    这里只清终态任务，且永远保留最新的 HISTORY_KEEP 条，避免刚下的任务立刻消失。
    必须在持有 TASKS_LOCK 时调用。
    """
    if len(TASKS) <= HISTORY_KEEP:
        return
    finals = [(tid, t) for tid, t in TASKS.items()
              if t.state in ("完成", "失败", "已取消")]
    overflow = len(TASKS) - HISTORY_KEEP
    if overflow <= 0:
        return
    # 终态任务按 id 升序（旧的在前）淘汰
    for tid, _ in sorted(finals, key=lambda kv: kv[0])[:overflow]:
        TASKS.pop(tid, None)


def _pick_queued():
    """把排队任务填满到 MAX_ACTIVE，且总连接数不超过 CONN_BUDGET。

    注意：一次要尽量填满，不能只启动一个 —— 多个任务同时结束时，
    每次只补一个会让队列推进速度落在完成速度后面，形成"半卡死"。

    P2-B：预算是乘法。只卡 MAX_ACTIVE=3 而允许每任务 64 连接 = 192 条 TCP，
    与"避免把带宽切碎"的注释直接矛盾。这里额外按【连接数】设上限：
    先算在跑任务已占用的连接（requested=0 表示自适应，按 4 估），
    剩余预算 >= 4 才允许再起一个任务。
    """
    with TASKS_LOCK:
        active = sum(1 for t in TASKS.values() if t.state == "下载中")
        used = sum((t.requested or 4) for t in TASKS.values() if t.state == "下载中")
        free = MAX_ACTIVE - active
        if free <= 0:
            return
        picked = []
        for t in TASKS.values():
            if t.state != "排队":
                continue
            need = t.requested or 4
            if used + need > CONN_BUDGET:
                if used >= 4:          # 剩余预算连自适应都放不下 → 等下一轮
                    break
                need = CONN_BUDGET - used
            used += need
            picked.append(t)
            if len(picked) >= free:
                break
        if not picked:
            return
        for t in picked:
            t.state = "下载中"        # 先在锁内占位，避免并发 _pick_queued 重复启动
    for t in picked:
        t.start()


def _target_key(url, kw):
    """推导任务的「目标产物」身份，用于去重。

    只比 URL 字符串会漏判：实测 ?u=1 / ?u=2 这种查询串变体指向同一文件，
    两个任务推导出同一个文件名 -> 必然撞 _FileLock。所以这里忽略查询串。

    但**不能退化成裸文件名** —— 文件名不是身份。上一版正是这么写的，
    结果 v1/app.zip 与 v2/app.zip、a.com/setup.exe 与 b.com/setup.exe
    被判为同一目标，第二个请求 add_task 直接返回 reused=True，面板显示"完成"，
    第二个文件从未下载且无任何报错（比撞锁更难发现，撞锁至少会红）。

    正确身份 = (host + path, 产物名)：host 与 path 都相同、只在查询串上不同
    才视为同一目标。"忽略 query"已经满足 ?u=1/?u=2 的原始诉求，不必牺牲 host/path。
    """
    u = urllib.parse.urlparse(url)
    name = kw.get("name")
    if not name:
        base = os.path.basename(u.path) or ""
        name = base or url
    return (u.netloc.lower() + u.path, name)


def _same_target(t, url, kw):
    if t.url == url:
        return True
    return _target_key(t.url, {"name": t.name}) == _target_key(url, kw)


def _meta_done_bytes(meta):
    return sum(max(0, e - s) for s, e in (meta.get("done") or []))


def _scan_resumable() -> int:
    """P1-C：启动时扫 DOWNLOAD_DIR/*.fluxdl，把残件重建为「可续传」任务。

    背景（实测）：服务重启后面板一片空白，但磁盘上躺着一堆
    "表观大小 == 完整大小"的文件（内核预分配过）+ 对应的 .fluxdl sidecar，
    而 sidecar 里 url/size/done 齐全 —— 恢复所需信息 100% 都在磁盘上，
    用户却只能靠文件名猜哪个下完了。

    这里只重建【元数据】任务（state="可续传"），不自动开下：
    重启后立刻满速跑起来可能是用户不想要的（他可能刚断网/在计费网络）。
    点「继续」才真正开跑。
    """
    try:
        names = [n for n in os.listdir(DOWNLOAD_DIR) if n.endswith(".fluxdl")]
    except OSError:
        return 0
    n_ok = 0
    with TASKS_LOCK:
        for meta_name in names:
            meta_path = os.path.join(DOWNLOAD_DIR, meta_name)
            part = meta_path[:-len(".fluxdl")]
            try:
                with open(meta_path, encoding="utf-8") as f:
                    meta = json.load(f)
                url = meta.get("url") or ""
                size = int(meta.get("size") or 0)
            except (OSError, ValueError, TypeError):
                continue                     # 坏 sidecar：跳过，不因为一个坏文件起不来
            if not url.lower().startswith(("http://", "https://")):
                continue
            if not os.path.exists(part):
                continue                     # 文件都没了，续传无从谈起
            done = _meta_done_bytes(meta)
            if size and done >= size:
                continue                     # 已经下满：只是没来得及提升成成品
            t = Task(url, name=os.path.basename(part))
            # D3：不补 out_path 的话 to_dict() 走 basename(out_path) → path 为空串，
            # 面板路径列空白，用户不知道这残件是哪个文件。resume 时内核会按同一
            # 路径续传（_derive_path 推导结果一致），所以直接指向半成品即可。
            t.out_path = part
            t.state = "可续传"
            t.done_bytes = done
            t.total_bytes = size
            t.meta_path = meta_path
            t.snapshot.update(done=done, total=size,
                              percent=round(100.0 * done / size, 2) if size else 0.0)
            # 「继续」时按剩余量重算并发，别用一个拍出来的数
            if size and done:
                t.requested = 0
            TASKS[t.id] = t
            n_ok += 1
    return n_ok


def resume_task(t) -> None:
    """P1-C：把「可续传/已取消/中断」重新排进队列。"""
    with TASKS_LOCK:
        if t.state not in ("可续传", "已取消", "中断", "失败"):
            return
        t.state = "排队"
        t.message = ""
        t.cancelled_before_start = False      # 内核按 sidecar 续传，从缺口继续
    _pick_queued()


def add_task(url, **kw):
    """新增任务。返回 (task, reused)。

    P0-3：同一 URL 只允许一个活动任务。
    内核用 _FileLock 对目标路径加排他锁，两个面板任务下载同一 URL 会推导出
    同一个文件名，于是后到的那个必然撞锁失败 —— 对用户来说就是"我点两下，
    第二下就红了"。正确做法不是放宽锁（那会产出一个大小对、内容全零的假成品），
    而是在面板层去重：已有活动任务就复用它。
    """
    with TASKS_LOCK:
        for t in TASKS.values():
            if t.state in ("排队", "下载中") and _same_target(t, url, kw):
                # P0-1：force_restart 的语义是「丢掉检查点重下」，
                # 复用一个正在跑的旧任务会让这个意图静默失效 ——
                # 用户以为重下了，其实还在按旧断点续传。
                if kw.get("force_restart"):
                    continue
                return t, True
        t = Task(url, **kw)          # 实例化放进锁内，避免 Task._seq 并发撞 id
        TASKS[t.id] = t
        _prune_tasks()               # P2-4：顺手淘汰过老的终态任务
    _pick_queued()
    return t, False


PAGE = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FluxDL</title>
<style>
:root{--bg:#fafaff;--card:#fff;--line:#eae7f5;--fg:#1f1f28;--dim:#8b8898;
      --accent:#6d4aff;--ok:#1a7f37;--err:#c9252d;--soft:#f4f2fc}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
     font:14px/1.6 -apple-system,"Segoe UI","Noto Sans CJK SC",sans-serif}
.wrap{max-width:860px;margin:0 auto;padding:40px 20px 80px}
h1{font-size:20px;font-weight:600;letter-spacing:.3px;margin:0 0 4px}
.sub{color:var(--dim);font-size:13px;margin-bottom:28px}
.row{display:flex;gap:10px;flex-wrap:wrap}
input[type=text],select{border:1px solid var(--line);background:var(--card);
     border-radius:8px;padding:10px 12px;font:inherit;color:inherit;outline:none}
input[type=text]{flex:1;min-width:240px}
input[type=text]:focus,select:focus{border-color:var(--accent)}
button{border:1px solid var(--line);background:var(--card);border-radius:8px;
     padding:10px 16px;font:inherit;cursor:pointer;transition:.15s}
button:hover{border-color:var(--accent);color:var(--accent)}
button.primary{background:var(--accent);border-color:var(--accent);color:#fff}
button.primary:hover{opacity:.9;color:#fff}
.meta{margin-top:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.meta input{font-size:13px;padding:7px 10px}
.tasks{margin-top:32px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;
      padding:14px 16px;margin-bottom:10px}
.card .top{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.name{font-weight:500;word-break:break-all}
.url{color:var(--dim);font-size:12px;word-break:break-all;margin-top:2px}
.badge{font-size:12px;padding:2px 9px;border-radius:999px;border:1px solid var(--line);
       white-space:nowrap;color:var(--dim);background:var(--soft)}
.badge.run{color:var(--accent);border-color:#ddd4ff;background:#f1edff}
.badge.done{color:var(--ok);border-color:#c6e8d0;background:#effaf2}
.badge.err{color:var(--err);border-color:#f5cdd0;background:#fdf1f2}
.bar{height:4px;background:var(--soft);border-radius:99px;margin:12px 0 8px;overflow:hidden}
.bar>i{display:block;height:100%;background:var(--accent);width:0;transition:width .25s}
.stats{display:flex;gap:16px;font-size:12px;color:var(--dim);flex-wrap:wrap}
.stats b{color:var(--fg);font-weight:500}
.mini{display:flex;gap:8px}
.mini button{padding:4px 10px;font-size:12px}
.empty{color:var(--dim);text-align:center;padding:40px 0;font-size:13px}
.msg{color:var(--err);font-size:12px;margin-top:6px;word-break:break-all}
.solidsub{font-size:12px;color:var(--dim);margin-top:14px}
.lock{margin-top:10px;display:flex;gap:10px;align-items:center;flex-wrap:wrap;
      font-size:12px;color:var(--dim)}
.lock input{flex:1;min-width:200px;font-size:13px;padding:7px 10px}
.lock button{padding:7px 14px;font-size:13px}
</style></head><body>
<div class="wrap">
  <h1>FluxDL</h1>
  <div class="sub">自研轻量下载内核 · 分段并发 / 断点续传 / HLS 分片合并</div>

  <div class="row">
    <input id="url" type="text" placeholder="粘贴链接（http/https/m3u8）" autofocus>
    <select id="conc">
      <option value="0">并发：自适应</option>
      <option value="1">1</option><option value="2">2</option>
      <option value="4">4</option><option value="8">8</option><option value="16">16</option>
    </select>
    <button class="primary" id="add">添加</button>
  </div>
  <div class="meta">
    <input id="referer" type="text" placeholder="Referer（可选，防盗链必填）" style="flex:1">
    <input id="cookie" type="text" placeholder="Cookie（可选）" style="flex:1">
  </div>
  <div class="solidsub" id="dirline"></div>
  <div class="lock" id="lockbar" hidden>
    <span>解锁：粘贴控制台打印的 token（只存本机浏览器 localStorage，页面本身不再内联 token）</span>
    <input id="tok" type="password" placeholder="token" autocomplete="off">
    <button id="unlock">解锁</button>
  </div>

  <div class="tasks" id="tasks"><div class="empty">暂无任务</div></div>
</div>
<script>
const $ = s => document.querySelector(s);
const TOK_KEY = 'fluxdl.token';

// P1-A（Hermes v3）：页面不再内联 token。
// 原先 "/" 不鉴权就把 token 明文写进 HTML，任何本机进程一条 curl 就能白拿，
// DNS rebinding 场景下外部页面读一下响应体也能拿到，token 等于没有。
// 现在第一次打开要手动粘贴一次（控制台启动时会打印），存 localStorage。
function loadTok(){ try { return localStorage.getItem(TOK_KEY) || ''; } catch(e){ return ''; } }
function saveTok(v){ try { localStorage.setItem(TOK_KEY, v); } catch(e){} }
let TOKEN = loadTok();

let dirShown = false;
function showDir(d){
  // "__DIR__" 仍然由服务端在【鉴权之后】的 /api/meta 下发，不再进页面模板
  if(dirShown) return;
  dirShown = true;
  document.getElementById('dirline').textContent = '保存到 ' + d;
}

function fmt(n){n=+(n||0);const u=['B','KB','MB','GB','TB'];let i=0;
  while(n>=1024&&i<u.length-1){n/=1024;i++}return n.toFixed(1)+u[i]}
function eta(s){if(s==null||!isFinite(s))return '--';s=Math.max(0,Math.round(s));
  return s<60?s+'s':Math.floor(s/60)+'m'+(s%60)+'s'}
function badge(st){const m={'下载中':'run','完成':'done','失败':'err'};
  return '<span class="badge '+(m[st]||'')+'">'+st+'</span>'}

// 所有 /api/* 都走这里：统一带 X-Token，403 时弹出解锁条。
async function api(path, opt){
  opt = opt || {};
  opt.headers = Object.assign({'X-Token': TOKEN}, opt.headers || {});
  const r = await fetch(path, opt);
  if(r.status === 403){ needToken(); }
  return r;
}

async function add(){
  const url = $('#url').value.trim();
  if(!url) return;
  $('#url').value='';
  await api('/api/add',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({
      url, referer:$('#referer').value.trim()||null,
      cookie:$('#cookie').value.trim()||null,
      concurrency:+$('#conc').value})});
}

// P1-C：把磁盘上还留着 sidecar 的残件恢复成任务（"继续"即可接着下）。
async function resume(id){
  await api('/api/resume?id=' + id, {method:'POST'});
}

// P2-8：差分更新。原先每 800ms 用 innerHTML 全量重建，
// 元素每次都是新建的，.bar>i 的 transition 永远没有起始宽度（进度条硬跳），
// 而且点「取消」时按钮正在被替换，有概率点空。
// 现在按 t.id 建一份 id -> 元素 的映射，只改变化行的文本和进度条宽度。
const ROWS = new Map();      // id -> {el, refs}

function rowHtml(t){
  return `<div class="top">
      <div style="min-width:0">
        <div class="name"></div>
        <div class="url"></div>
      </div>
      <div class="mini"><span class="badge"></span>
        <button data-act="resume" hidden>继续</button>
        <button data-act="cancel">取消</button>
      </div>
    </div>
    <div class="bar"><i></i></div>
    <div class="stats">
      <span><b class="s-pct"></b></span>
      <span class="s-size"></span>
      <span class="s-speed"></span>
      <span class="s-eta"></span>
      <span class="s-conn"></span>
    </div>
    <div class="msg"></div>`;
}

function makeRow(t){
  const el = document.createElement('div');
  el.className = 'card';
  el.innerHTML = rowHtml(t);
  const refs = {
    name: el.querySelector('.name'), url: el.querySelector('.url'),
    badge: el.querySelector('.badge'), bar: el.querySelector('.bar>i'),
    pct: el.querySelector('.s-pct'), size: el.querySelector('.s-size'),
    speed: el.querySelector('.s-speed'), eta: el.querySelector('.s-eta'),
    conn: el.querySelector('.s-conn'), msg: el.querySelector('.msg'),
    btn: el.querySelector('button[data-act=cancel]'),
    resume: el.querySelector('button[data-act=resume]'),
  };
  refs.btn.addEventListener('click', () => cancel(t.id));
  refs.resume.addEventListener('click', () => resume(t.id));
  ROWS.set(t.id, {el, refs});
  return ROWS.get(t.id);
}

function render(list){
  const box = $('#tasks');
  const empty = box.querySelector('.empty');
  if(!list.length){
    if(!empty){ box.innerHTML = '<div class="empty">暂无任务</div>'; }
    ROWS.clear();
    return;
  }
  if(empty) empty.remove();

  const seen = new Set();
  let prev = null;
  for(const t of list){
    seen.add(t.id);
    let row = ROWS.get(t.id);
    if(!row){ row = makeRow(t); }
    // 保持顺序：把节点按返回顺序排列（appendChild 会移动已存在节点）
    box.appendChild(row.el);
    prev = row;
    updateRow(row, t);
  }
  for(const [id, row] of ROWS){
    if(!seen.has(id)){ row.el.remove(); ROWS.delete(id); }
  }
}

const RESUME_STATES = new Set(['可续传','已取消','中断']);
function updateRow(row, t){
  const r = row.refs;
  const pct = Math.min(t.percent||0, 100);
  const live = t.state === '下载中';

  const name = t.name || (t.url||'').split('/').pop();
  if(r.name.textContent !== name) r.name.textContent = name;
  if(r.url.textContent !== t.url) r.url.textContent = t.url;

  if(r.badge.textContent !== t.state){
    r.badge.textContent = t.state;
    r.badge.className = 'badge ' + ({'下载中':'run','完成':'done','失败':'err'}[t.state] || '');
  }
  const canCancel = live || t.state === '排队';
  if(r.btn.style.display !== (canCancel ? '' : 'none'))
    r.btn.style.display = canCancel ? '' : 'none';
  // P1-C：中断/已取消/可续传 的任务给一个「继续」，不必重填 URL
  const canResume = RESUME_STATES.has(t.state);
  if(r.resume.style.display !== (canResume ? '' : 'none'))
    r.resume.style.display = canResume ? '' : 'none';

  r.bar.style.width = pct.toFixed(2) + '%';        // 只改宽度，transition 才有效

  r.pct.textContent = pct.toFixed(1) + '%';
  r.size.textContent = fmt(t.done) + ' / ' + (t.total ? fmt(t.total) : '未知');
  r.speed.textContent = fmt(t.speed) + '/s';
  r.eta.textContent = '剩 ' + eta(t.eta);
  r.conn.textContent = '连接 ' + (t.concurrency || 1);

  const msg = t.message || '';
  if(r.msg.textContent !== msg) r.msg.textContent = msg;
  r.msg.style.display = msg ? '' : 'none';
}

function esc(s){return String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}

async function cancel(id){await api('/api/cancel?id='+id,{method:'POST'})}

$('#add').onclick = add;
$('#url').addEventListener('keydown',e=>{if(e.key==='Enter')add()});

// P2-5：原先这里在 800ms 轮询 /api/list，而服务端 /api/events 的 SSE 早就写好了
// 却没人订阅 —— 等于死代码 + 白跑一份长连接实现。改成订阅 SSE：
// 服务端 0.7s 推一次，网络上有新数据才动，空闲时不会像轮询那样每隔 800ms
// 重建一次 HTTP 请求（每个请求一个线程，ThreadingHTTPServer 下尤其浪费）。
// EventSource 不能自定义 header，所以 token 走 query（仅本机 127.0.0.1 监听）。
let es = null, sseOk = false, started = false;
let known = new Map();     // id -> task（增量合并后的本地视图）
let prevActive = [];       // 上一帧的活跃任务 id（用于删除时补一次全量）

// P1-A：EventSource 不能设请求头，token 只能走 query。
// 但 SSE 只用于【展示】，所以服务端只接受一次性 nonce（启动时打印，10 分钟过期，
// 用过即作废），而不是长期的 X-Token —— 这样即使 URL 进了日志/历史记录也不致命。
let sseStarted = false;
let pingTimer = null;

function applyList(tasks){ known = new Map(tasks.map(t => [t.id, t])); render(tasks); }

// ---------------------------------------------------------------- P1-B 增量协议
// 服务端改推 {v, changed, removed}：只有变化的任务进 changed，版本没变就只发 ": ping"。
// EventSource 对注释行不派发 message 事件，所以前端零感知。
function applyPatch(p){
  let structural = false;
  for(const t of (p.changed || [])){
    const old = known.get(t.id);
    // 状态变化会影响排序（活跃在前、终态按 id 倒序）→ 走一次全量 render
    if(old && old.state !== t.state) structural = true;
    known.set(t.id, Object.assign(old || {}, t));
  }
  for(const id of (p.removed || [])){ if(known.delete(id)) structural = true; }

  const list = [...known.values()];
  const active = list.filter(t => t.state === '下载中' || t.state === '排队').map(t => t.id);
  if(structural || String(active) !== String(prevActive)){
    prevActive = active;
    render(list);
  } else {
    for(const t of (p.changed || [])){
      const row = ROWS.get(t.id);
      if(row) updateRow(row, known.get(t.id));
    }
  }
}

function pollFallback(){
  // SSE 不可用（老浏览器 / 代理掐长连接）时退回轮询，功能不能没有。
  api('/api/list').then(r=>r.json()).then(d=>{
    applyList(d.tasks);
    if(d.dir) showDir(d.dir);
  }).catch(()=>{});
}

function connectSSE(){
  if(!window.EventSource){ setInterval(pollFallback, 800); return; }
  if(sseStarted) return;
  api('/api/nonce', {method:'POST'}).then(r=>{
    if(!r.ok) throw new Error('nonce');
    return r.json();
  }).then(d=>{
    sseStarted = true;
    if(d && d.dir) showDir(d.dir);
    es = new EventSource('/api/events?nonce=' + encodeURIComponent(d.nonce));
    es.onmessage = ev => {
      try {
        const p = JSON.parse(ev.data);
        if(p.full) applyList(p.full);
        else applyPatch(p);
        if(p.dir) showDir(p.dir);
      } catch(e) {}
    };
    es.onerror = () => {
      // 服务端重启 / nonce 过期：EventSource 会拿旧 URL 不停重连并被 403，
      // 所以这里主动断开，等解锁后重新握手。
      sseOk = false;
      if(es){ es.close(); es = null; }
      setTimeout(() => { sseStarted = false; if(TOKEN) connectSSE(); }, 1500);
      pollFallback();
    };
  }).catch(()=>{});
}

// ---------------------------------------------------------------- 解锁流程
function needToken(){
  $('#lockbar').hidden = false;
  const i = $('#tok'); if(i) i.focus();
}
function boot(){
  if(!TOKEN){ needToken(); return; }
  started = true;
  $('#lockbar').hidden = true;
  pollFallback();          // 先拿一帧全量（顺带校验 token 是否有效）
  connectSSE();
}
$('#unlock').onclick = () => {
  const v = $('#tok').value.trim();
  if(!v) return;
  TOKEN = v;
  saveTok(v);
  if(es){ es.close(); es = null; }
  sseStarted = false;
  boot();
};
$('#tok').addEventListener('keydown', e => { if(e.key === 'Enter') $('#unlock').click(); });
boot();
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    server_version = "FluxDL"
    timeout = 30            # P2-C：不给 None。否则半开连接能被无限期挂住

    def log_message(self, fmt, *args):      # 静音，避免刷屏
        pass

    def _host_ok(self):
        """P1-A①：Host 头白名单。

        只监听 127.0.0.1 时，正常访问的 Host 必然是 127.0.0.1:PORT /
        localhost:PORT / [::1]:PORT。DNS rebinding 的攻击页面把 evil.com
        解析到 127.0.0.1，但浏览器发出的 Host 仍是 evil.com:PORT —— 一条就能挡。
        """
        return (self.headers.get("Host") or "") in ALLOWED_HOSTS

    def _auth_ok(self, allow_query=False):
        # P2-8：用 hmac.compare_digest 做常量时间比较。
        # 原先的 == 会在第一个不同字节处提前返回，理论上可被计时侧信道逐字节猜出 token
        # （本机单用户工具风险很低，但改一行的事没理由不做）。
        got = self.headers.get("X-Token") or ""
        if got:
            return hmac.compare_digest(got, TOKEN)
        # P2-5：EventSource 无法自定义请求头，SSE 只能用 query 传凭证。
        # P1-A②：这里认的是【一次性 nonce】，不是长期 token ——
        # URL 会进浏览器历史/代理日志，长期凭证不该出现在那里。
        if allow_query:
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            got = (q.get("nonce") or [""])[0]
            return bool(got) and _burn_nonce(got)
        return False

    @staticmethod
    def _int_param(raw, default=0, lo=None, hi=None):
        """把外部输入安全地转成 int，失败返回 None（调用方回 400）。

        P2-3：原先 int(q.get("id"))、int(data.get("concurrency")) 直接解析外部输入，
        随手发一个 /api/cancel?id=abc 或 {"concurrency": "lots"} 就会抛 ValueError，
        被 BaseHTTPRequestHandler 兜成 500 + 整段 traceback 写到控制台和响应里。
        这既是信息泄露，也让"用户输错一个字符"看起来像服务器崩了。
        """
        if raw is None or raw == "":
            return default
        try:
            v = int(str(raw).strip())
        except (TypeError, ValueError):
            return None
        if lo is not None and v < lo:
            return None
        if hi is not None and v > hi:
            return None
        return v

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, text, code=200):
        body = text.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        # P1-A①：所有请求（含静态页）先过 Host 白名单。
        if not self._host_ok():
            return self._json({"error": "Host 不被允许"}, 403)
        if u.path in ("/", "/index.html"):
            # P1-A②：页面是【静态】的 —— 不再内联 token，也不再内联下载目录。
            # 原先 curl 一条无鉴权 GET / 就能读到 TOKEN=...，token 形同虚设；
            # 目录同理（泄露本机路径/用户名）。
            return self._html(PAGE)
        # SSE 必须放在通用 token 校验之前：EventSource 无法自定义请求头，
        # 凭证只能走 query（nonce），而通用 _auth_ok() 只认 X-Token 头。
        if u.path == "/api/events":           # SSE 实时推送
            if not self._auth_ok(allow_query=True):
                return self._json({"error": "需要 token"}, 403)
            return self._sse()
        if not self._auth_ok():
            return self._json({"error": "需要 token"}, 403)
        if u.path == "/api/list":
            with TASKS_LOCK:
                ts = sorted(TASKS.values(), key=lambda t: -t.id)
            return self._json({"tasks": [t.to_dict() for t in ts], "dir": DOWNLOAD_DIR})
        if u.path == "/api/meta":
            return self._json({"dir": DOWNLOAD_DIR, "max_active": MAX_ACTIVE,
                               "conc_max": CONC_MAX, "budget": CONN_BUDGET})
        return self._json({"error": "not found"}, 404)

    # ------------------------------------------------------------------ SSE
    def _sse(self):
        """P1-B：增量协议 + P2-C：客户端上限与 socket 超时。

        原实现每 0.7s 在锁内把【全部任务】to_dict() 一遍再 json.dumps 全量下发：
        200 个任务时实测 66,211 B/次（1.43 Hz → 92.5 KB/s/客户端），
        且 O(客户端数 × 任务数) 的 GIL 争用。开 3 个标签页就是 3 份。

        新协议：
          {"v": 版本号, "changed": [变化任务], "removed": [id]}
          - 版本没变 → 只写一行 ": ping"（EventSource 忽略注释行）
          - 变化字段做 delta：不变字段（url/path…）不再每帧重发
          - 新连接第一帧带 "full": [...]，前端可直接重建视图
        """
        global _SSE_ACTIVE
        with _SSE_LOCK:
            if _SSE_ACTIVE >= SSE_MAX:
                return self._json({"error": f"SSE 客户端已达上限 {SSE_MAX}"}, 503)
            _SSE_ACTIVE += 1
        try:
            # P2-C：settimeout 让"客户端拔网线"在 write 时立刻抛错，
            # 而不是靠下一次 flush 才发现；也让 idle 客户端最终能回收。
            try:
                self.connection.settimeout(10)
            except OSError:
                pass
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            # 每个连接各自维护"上次发出去的视图"，只推差异
            sent: dict[int, dict] = {}
            first = True
            while True:
                with TASKS_LOCK:
                    cur = {t.id: t.to_dict() for t in TASKS.values()}
                changed = [d for tid, d in cur.items()
                           if d != sent.get(tid)]
                removed = [tid for tid in sent if tid not in cur]
                if first:
                    payload = {"full": [cur[k] for k in sorted(cur, reverse=True)],
                               "dir": DOWNLOAD_DIR}
                    first = False
                elif changed or removed:
                    # delta：只带变化字段（前端 Object.assign 合并）
                    body = []
                    for d in changed:
                        old = sent.get(d["id"])
                        body.append(d if old is None else
                                    {k: v for k, v in d.items()
                                     if k == "id" or old.get(k) != v})
                    payload = {"changed": body, "removed": removed}
                else:
                    self.wfile.write(b": ping\n\n")        # 心跳，不算事件
                    self.wfile.flush()
                    time.sleep(SSE_INTERVAL)
                    continue
                sent = cur
                self.wfile.write(("data: " + json.dumps(payload, ensure_ascii=False)
                                  + "\n\n").encode())
                self.wfile.flush()
                time.sleep(SSE_INTERVAL)
        except Exception:
            return
        finally:
            with _SSE_LOCK:
                _SSE_ACTIVE -= 1

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        if not self._host_ok():
            return self._json({"error": "Host 不被允许"}, 403)
        if u.path == "/api/nonce":
            # P1-A②：SSE 专用的一次性凭证。必须先有合法 X-Token 才发得出来，
            # 所以 rebinding 页面拿不到（它连 X-Token 都写不进跨域请求头）。
            if not self._auth_ok():
                return self._json({"error": "需要 token"}, 403)
            return self._json({"nonce": _new_nonce(), "dir": DOWNLOAD_DIR,
                               "ttl": NONCE_TTL})
        if not self._auth_ok():
            return self._json({"error": "需要 token"}, 403)
        if u.path == "/api/resume":           # P1-C：继续一个可续传任务
            q = urllib.parse.parse_qs(u.query)
            tid = self._int_param((q.get("id") or ["0"])[0], default=0, lo=0)
            if tid is None:
                return self._json({"error": "id 必须是正整数"}, 400)
            with TASKS_LOCK:
                t = TASKS.get(tid)
            if not t:
                return self._json({"error": "任务不存在"}, 404)
            if t.state not in ("可续传", "已取消", "中断", "失败"):
                return self._json({"error": f"当前状态 {t.state} 不能继续"}, 400)
            resume_task(t)
            return self._json({"ok": True})
        if u.path == "/api/cancel":
            q = urllib.parse.parse_qs(u.query)
            tid = self._int_param((q.get("id") or ["0"])[0], default=0, lo=0)
            if tid is None:
                return self._json({"error": "id 必须是正整数"}, 400)
            with TASKS_LOCK:
                t = TASKS.get(tid)
            if t:
                t.cancel()
            return self._json({"ok": True})
        if u.path == "/api/add":
            n = int(self.headers.get("Content-Length") or 0)
            try:
                data = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._json({"error": "非法 JSON"}, 400)
            if not isinstance(data, dict):
                return self._json({"error": "请求体必须是 JSON 对象"}, 400)
            conc = self._int_param(data.get("concurrency"), default=0, lo=0, hi=CONC_MAX)
            if conc is None:
                return self._json(
                    {"error": f"concurrency 必须是 0-{CONC_MAX} 的整数"}, 400)
            url = (data.get("url") or "").strip()
            if not url.lower().startswith(("http://", "https://")):
                return self._json({"error": "只支持 http/https"}, 400)
            t, reused = add_task(url, referer=data.get("referer") or None,
                                 cookie=data.get("cookie") or None,
                                 name=data.get("name") or None,
                                 concurrency=conc,
                                 force_restart=bool(data.get("force_restart")))
            return self._json({"ok": True, "id": t.id, "reused": reused})
        return self._json({"error": "not found"}, 404)


def _shutdown(srv, signame="SIGINT"):
    """P3：统一收尾。

    原先只捕 KeyboardInterrupt —— systemd / docker / kill 默认发的是 SIGTERM，
    进程直接死，日志里连 "bye" 都没有，用户看到的是"服务莫名消失"。
    数据本身是安全的（_atomic_json 落盘），但体验很粗糙：没人告诉用户断点还在。

    P3-2（本轮实测发现）：只调 t.cancel() 然后立刻 SystemExit 是【假收尾】。
    cancel() 只是 stop_flag.set() 就返回，下载线程还在 _worker 里；主线程马上
    raise SystemExit 把进程带走（线程是 daemon），内核那条
    "_run_ranged 循环退出 → _flush_meta()" 的路径根本没机会执行 →
    .fluxdl 一个字节都没写。而日志照样打印"断点已保存"，说谎。
    实测证据：SIGTERM 后 dl 目录为空，重启扫到 0 个可续传任务。
    正确做法：cancel 之后 join 线程，给内核留出收尾时间。
    """
    stopped = saved = 0
    with TASKS_LOCK:
        live = [t for t in TASKS.values() if t.state in ("排队", "下载中")]
    for t in live:
        try:
            t.cancel()
            stopped += 1
        except Exception:
            pass

    # 给内核时间把最后一帧 pending 区间合并进 .fluxdl 并原子落盘
    deadline = time.time() + SHUTDOWN_GRACE
    for t in live:
        th = getattr(t, "thread", None)
        if th is None or not th.is_alive():
            continue
        th.join(max(0.0, deadline - time.time()))

    for t in live:
        th = getattr(t, "thread", None)
        if th is not None and th.is_alive():
            continue                      # 超时未收尾 —— 下面单独告警
        mp = getattr(t, "meta_path", "") or (
            (getattr(t.dl, "meta_path", "") if t.dl else ""))
        if mp and os.path.exists(mp):
            saved += 1

    if stopped:
        if saved >= stopped:
            print(f"已停止 {stopped} 个任务，断点已保存（.fluxdl），"
                  f"重跑同命令/点「继续」可续传")
        elif saved:
            print(f"已停止 {stopped} 个任务，其中 {saved} 个断点已保存；"
                  f"{stopped - saved} 个未完成收尾（该任务需重新下载）")
        else:
            print(f"已停止 {stopped} 个任务，但断点未能落盘"
                  f"（sidecar 缺失，续传不可用）")
    else:
        print("无进行中的任务")
    # D1：这里【不能】再调 srv.shutdown()。
    # socketserver.BaseServer.shutdown() 的实现是：
    #     self.__shutdown_request = True
    #     self.__is_shut_down.wait()      # 等 serve_forever() 退出时 set
    # 而 serve_forever() 只有【主线程】自己在跑。若在信号处理函数（主线程）里
    # 同步调 shutdown()，等的人和干活的是同一个线程 → 永久互等，
    # /proc/<pid>/task/*/wchan 全是 futex_do_wait（实测挂死 20s+，只能 kill -9）。
    # 关 server 的动作交给 main() 在 serve_forever() 返回之后做，或在独立线程里做。


def _request_shutdown(srv):
    """在【独立线程】里请求 serve_forever() 退出（D1）。"""
    try:
        srv.shutdown()
    except Exception:
        pass


def _line_buffer_stdio():
    """D2：非 tty（systemd / docker / 重定向到文件）时 stdout 是全缓冲的。

    横幅里有 token，而 P1-A 之后 token 只能手动粘贴 —— 日志里迟迟不出现
    「token: ...」等于面板不可用；排障时 docker logs 也是空的。
    reconfigure 在 Py3.7+ 可用，老版本/异常环境退回 flush=True 兜底。
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(line_buffering=True)
        except Exception:
            pass


def main():
    _line_buffer_stdio()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    srv.daemon_threads = True
    print(f"FluxDL 面板: http://{HOST}:{PORT}/", flush=True)
    print(f"token: {TOKEN}", flush=True)
    print(f"目录: {DOWNLOAD_DIR}", flush=True)
    n = _scan_resumable()
    if n:
        print(f"发现 {n} 个可续传任务（磁盘上的 .fluxdl），面板里点「继续」即可")

    def _on_signal(signum, frame):
        # D1：信号处理函数在【主线程】执行，而主线程此刻正阻塞在 serve_forever()。
        # 所以这里只做两件事：停任务（可以直接做）+ 把 shutdown 丢到独立线程。
        # 不能在这里 srv.shutdown()（同线程自等死锁），也不能 raise SystemExit
        # （那会跳过收尾，且 SystemExit 在别的线程里 raise 只结束该线程）。
        name = signal.Signals(signum).name
        try:
            _shutdown(srv, name)
        finally:
            threading.Thread(target=_request_shutdown, args=(srv,),
                             name="fluxdl-shutdown", daemon=True).start()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _on_signal)
        except (ValueError, OSError):
            pass          # 非主线程 / 不支持的平台：忽略，别因此起不来
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        # 兜底：没装成信号处理器时的 Ctrl-C
        _shutdown(srv, "SIGINT")
    finally:
        # serve_forever() 已返回 → 主线程不再阻塞，这里确保 socket 关掉。
        # join 给 shutdown 线程一点时间（它可能刚启动），但绝不无限等。
        try:
            srv.server_close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

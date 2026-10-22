# FluxDL 复审报告 v2（Hermes）

日期：2026-10-22
评审范围：`<repo>/`（本地）+ `<repo-url>`（远端）
基线：`代码评审-Hermes.md`（上一轮）、`BUG修复记录.md`（P0-1/P0-2/P0-3/P1-4/P1-5/P2-6/P2-8/稳定性修复）
性质：**增量评审**，只报上一轮之后新出现或仍未修的问题，不复述已修复项。

---

## 0. 一句话结论

上一轮的 P0 修得不错（空洞检测用 SEEK_HOLE、预分配识别、内容校验），但**两条"静默成功"的路径仍然通着**，
且其中一条是上一轮修复自己引入的回归。核心教训"不要只信文件存在/大小"还没有贯彻到底——
现在的情况是：**文件大小对、内容校验也"通过"，但它是残的。**

---

## 1. P0-1：截断文件被当成成品，全链路报成功（已实测复现）

### 复现方法

本地起一个 socket 服务器：`Content-Length: 100000`，实际只发 60000 字节后优雅关闭；
再让它不支持 Range（不返 `Accept-Ranges`），使内核走 `_run_single()` 单连接路径。

### 实测结果

```
run() 返回成功: {'path': '.../half.bin', 'kind': 'file', 'size': 60000}
落盘大小: 60000
verify_file: (True, 'ffb240dcc910eace...')
```

服务器声明 10 万字节，磁盘上 6 万字节，`verify_file()` 返回 **True**，CLI 会打印
`校验通过: sha256=...` + `完成: ...`。**没有任何一处发现少了 40%，退出码是 0。**

### 根因

`_run_single()`（`fluxdl.py`）：

```python
cl = r.headers.get("Content-Length")
if cl and cl.isdigit():
    self.total = int(cl)          # total=100000
...
    buf = r.read(65536)
    if not buf:
        break                     # ← EOF 静默 break，n=60000
os.replace(part, self.path)
return {"path": self.path, "kind": "file", "size": n}   # 不比对 total
```

`verify_file()` 的三道检查全都看不到这个损坏：
- `_find_holes()` → 不是稀疏文件，无空洞；
- 全零块抽样 → 数据是真的 0x01，非零；
- gzip 魔数 → 非 gz 文件，跳过；
- sha256 → 没有 `expect` 时**只算不比对**（见 P1-4）。

### 影响

无 Range 的服务器很常见（CDN 边缘节点、部分对象存储签名 URL、反向代理配错）。
网络抖动、代理超时、服务端限速掐连接，都会走到这条路径。**用户拿到一个能打开但内容残缺的
zip/安装包/pdf，报错为零。**

### 修复（一行）

```python
if self.total and n != self.total:
    raise DownloadError(f"响应被截断：Content-Length={self.total}，实收 {n} 字节")
```

放在 `os.replace()` **之前**，并删掉 `.part`。Range 路径有逐段 `got < take` 检查，
不受影响。

---

## 2. P0-2：按文件名去重，不同 URL 的同名文件被静默拒绝（已实测复现）

### 实测结果

```
a.com/v1/pack.zip -> pack.zip
b.com/v2/pack.zip -> pack.zip
判为同一目标: True
_same_target(不同URL同名) -> True
```

### 根因

`fluxdl_server.py`：

```python
def _target_key(url, kw):
    name = kw.get("name")
    if name: return name
    return os.path.basename(urllib.parse.urlparse(url).path) or url
```

这是修 P0-3（同 URL 点两下必撞 `_FileLock`）时引入的。为了兜住 `?u=1` / `?u=2`
指向同一文件的场景，把去重键降级成了裸文件名——**但文件名不是身份**。

### 后果

`https://github.com/x/proj/releases/download/v1/app.zip` 和
`.../v2/app.zip`、`a.com/setup.exe` 和 `b.com/setup.exe`：第二个请求
`add_task()` 直接返回 `reused=True`，面板显示"完成"，**第二个文件从未下载，无任何报错**。
这是比撞锁更难发现的失败——撞锁至少会红。

### 修复

去重键改为 `(final_url_without_query, out_path)`，或更稳的做法：
**用 `(netloc + path, name)`**，只在 host 相同且 path 相同（忽略查询串）时判定同一目标。
`?u=1`/`?u=2` 的原始诉求用"忽略 query"就满足了，不需要牺牲 host 和 path。

---

## 3. P1：任务槽位泄漏与幽灵取消（代码确证）

### P1-1 `Downloader(...)` 构造在 try 之外 → 线程死亡后永不归还名额

`fluxdl_server.py` `Task._run()`：

```python
def _run(self):
    dl = fluxdl.Downloader(...)     # ← try 之外
    self.dl = dl
    try:
        res = dl.run()
    ...
    finally:
        _pick_queued()              # ← 属于 try，构造抛异常时不会执行
```

构造抛异常（URL 非法、`int()` 失败、`HttpPool` 初始化失败）→ 线程异常退出 →
`finally` 不执行 → `_pick_queued()` 不被调用：
1. `state` 永久停在 `_pick_queued()` 里刚设的"下载中"；
2. `MAX_ACTIVE=3` 的计数只增不减，**累计 3 次之后面板再也起不了新任务**，只能重启。

修复：把构造挪进 `try`，或再加一层外层 `try/finally` 保证 `_pick_queued()` 一定执行。

### P1-2 取消竞态：点了取消，文件继续下完，最后报"完成"

`_pick_queued()` 是先置 `state="下载中"` 再 `t.start()`；`cancel()` 在 `self.dl is None`
时只改 `state="已取消"`。窗口期内取消 → 线程起来照常建 Downloader 并 `run()` 到底 →
结束时把 `state` 覆盖为"完成"。

修复：`_run()` 开头检查一次取消标志（`if self.state == "已取消": return`），
`run()` 结束后写状态前再检查一次；`cancel()` 对 `dl is None` 的情况置一个
`self.cancelled_before_start = True`，让线程启动后立刻退出。

---

## 4. P1：内容校验与探测路径上的三个隐患（代码确证）

### P1-3 `probe()` 的 HEAD 回退把整个文件读进内存

`fluxdl.py`：

```python
except Exception:                       # HEAD 不被支持
    conn, r, final = pool.open(url, rng=(0, 0))
    ...
    pool.drain(r)                       # = resp.read()，无上限
```

`drain()` 是 `resp.read()`。而进入这条分支的前提正是"服务器不标准"——忽略 `Range`
返回 200 全量的服务器，会把 1.5GB 文件一次读进内存。HEAD 成功那条路径安全（无 body），
偏偏回退路径危险。`_run_single()` 的 `finally: self.pool.drain(r)` 也有同样问题：
异常收尾时把剩余 body 全读完，可能二次卡死。

修复：探测只读响应头，用 `r.close()` / `conn.close()`；`drain()` 加 `max_bytes` 上限
（如 64KB）后断开。

### P1-4 没有 `--expect-sha256` 时照样全量算 sha256

`verify_file()` 末尾无条件 `actual = _hash_file(path, algo)`，只在 `if expect:` 时才用。
一个 200MiB 文件，下载本身 1.8s（14.2×），sha256 反而要 1–2s——**自己把加速吃掉一半**。
10GiB 文件更明显（几十秒）。

修复：`if expect:` 才算全文摘要；无 expect 时只做存在性 + 空洞 + 抽样 + gzip 魔数。

### P1-5 HLS 忽略 `#EXT-X-KEY` 与 `#EXT-X-MAP`，合并出"成功"的坏视频

`_parse_m3u8()` 把非 `#` 开头的行当分片：
- `#EXT-X-KEY:METHOD=AES-128,URI=...` 被丢掉 → 下到的是密文分片；
- `#EXT-X-MAP:URI="init.mp4"` 被丢掉 → fMP4 流 concat 出来没有 moov。

两种情况下 `ffmpeg -c copy` 都可能返回 0 → 面板"完成"。至少要：
1. 检测到 `#EXT-X-KEY` 且 `METHOD != NONE` → 明确报"加密流不支持"，别装成功；
2. 检测到 `#EXT-X-MAP` → 把 init segment 作为首个输入拼进去，或直接拒绝。

另外 `subprocess.run(["ffmpeg", ...])` 没有 `timeout`，ffmpeg 卡住 → 任务永久"合并中"
且不响应取消。

---

## 5. P2：性能与稳健性

| # | 问题 | 位置 | 建议 |
|---|---|---|---|
| P2-1 | 小文件也做 1.5s/3MB 自适应测速，净亏损 | `_measure_and_pick()` | `total < 8MB` 时跳过测速，固定 4 并发 |
| P2-2 | `_run_single()` 不持 `_FileLock`，两个进程写同一 `.part` 交错覆写 | `_run_single` | 锁提到 `run()` 外层统一加 |
| P2-3 | 面板所有外部输入无 try：`int(q.get("id"))`、`int(data.get("concurrency"))` 遇非数字直接 500+traceback | `fluxdl_server.py` | 统一 `_bad_request()` → 400 |
| P2-4 | `TASKS` 只增不减，`/api/list` 全量返回，`ROWS` 只差分不删 | 服务端 | 保留最近 200 条 + 已完成任务可清理 |
| P2-5 | `/api/events`（SSE）写完了但前端 `tick()` 用 800ms 轮询，SSE 是死代码 | 前后端 | 接上 SSE 并删轮询，或删 SSE |
| P2-6 | `MAX_ACTIVE=3 × max_concurrency=16 = 48` 连接，与 CLI `-c` 无联动 | 全局 | 加全局连接预算（如总 32） |
| P2-7 | `--cookie` 明文进 argv，`ps aux` 全机可见 | CLI | 改 env 或 `--cookie-file` |
| P2-8 | `TOKEN` 内联进 HTML，本机任何进程可取；`_auth_ok` 非常量时间比较 | 服务端 | `hmac.compare_digest`；README 声明"单用户本机工具" |
| P2-9 | 短读重插无重试上限，错误信息"仍有区间未完成"无诊断价值 | `_run_ranged` | 记录失败区间起点与错误原文 |
| P2-10 | `_find_holes` 在 tmpfs/网络 fs 上返回 None 时静默退化，只剩 3 处抽样 | `verify_file` | verbose 下打一行"空洞检测不可用" |

---

## 6. 远端（GitHub）现状

### 已同步

- `fluxdl.py` — **SAME**（与本地一致）
- `fluxdl_server.py` — **SAME**

### 未同步

| 文件 | 本地 | 远端 |
|---|---|---|
| `README.md` | 88 行 | 80 行（旧版） |
| `性能优化记录.md` | 238 行 | 226 行 |

### 完全没入库（本地未提交）

`BUG修复记录.md`、`代码评审-Hermes.md`、`FLARE问题记录.md`
—— 这三份是项目最值钱的东西（事故档案 + 修复对照 + 外部评审），
恰恰没进仓库。别人 clone 下来只能看到"一个下载器"，看不到"为什么这么写"。

### 工程化

- `.gitignore` — **有**（上一轮说的是"缺"，此处更正）
- `LICENSE` — **缺**。公开仓库没有授权 = 默认保留一切权利，别人连合法 fork 都算不上。
- `requirements.txt` / `pyproject.toml` — 缺。纯标准库是优点，应在 README 明说
  "零第三方依赖，仅 HLS 合并需 ffmpeg"，否则用户不敢用。
- `CHANGELOG.md` — 缺。
- CI / 测试 — 缺。而 `BUG修复记录.md` 里每个 bug 都可写成断言。本报告第 1、2 节的
  两个 bug 我已经用 60 行脚本复现了，就是现成的回归测试。

---

## 7. 建议的修复顺序

1. `_run_single()` 加 `n != total` 断言 → 堵死静默截断（P0-1，一行）
2. `_target_key()` 改 `(host + path, name)` → 修同名误杀（P0-2，三行）
3. `Task._run()` 构造挪进 `try`（P1-1）
4. `cancel()` 双向检查（P1-2）
5. `probe()` 去 drain / `drain` 加上限（P1-3）
6. `verify_file()` 无 expect 跳过 sha256（P1-4）
7. `#EXT-X-KEY` 非 NONE 直接报错（P1-5）
8. 补 LICENSE + 3 个回归测试 + 同步 README/性能记录 + 事故档案入库

前 7 条都是小改动、无架构风险。第 8 条是把"能跑的脚本"变成"别人能用的项目"的分界线。

---

## 附：复现脚本

本报告第 1、2 节的结论来自 `<scratch>/test_fluxdl.py`
（socket 服务器 + 直接 import 内核），运行方式：

```bash
python3 <scratch>/test_fluxdl.py
```

建议把它改写成 pytest 放进仓库 `tests/`，作为这两个 P0 的回归测试。

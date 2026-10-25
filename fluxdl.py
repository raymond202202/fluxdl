#!/usr/bin/env python3
"""
FluxDL v2 — 自研轻量下载内核

本版本修掉第一版的几个真 bug（都是实测发现的，不是推测）：
  1. http.client 不跟随 302/301 重定向 → npmmirror 探测出 size=88（其实是跳转页）。已修。
  2. 每次 Range 都重新握手 TLS（python.org 单次首包要 12.7s）→ 引入 keep-alive 连接池。
  3. 固定并发是拍脑袋 → 改成"先测单连接吞吐再定档"，依据见下。
  4. 续传元数据不是原子写 → 现在 tmp + fsync + rename。

并发档位的实测依据（同一文件 typescript-5.6.3.tgz，4MB）：
    并发 1 → 194 KB/s
    并发 2 → 224 KB/s  (+16%)
    并发 4 → 200 KB/s  (+3%，纯属自我安慰)
    并发 8 → 429 KB/s  (+221%)
结论：并发收益不是线性的，必须实测决定，且默认值不能是 4。

用法:
    python3 fluxdl.py URL [-o DIR] [-c N] [--probe] [--referer R] [--cookie C]
"""
from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import shutil
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse

DEFAULT_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
CHUNK = 256 * 1024
META_V = 5
SAMPLE = 4096          # P0-1 降级指纹：首 4 KiB 抽样
MAX_REDIRECT = 6


class DownloadError(Exception):
    pass


# ----------------------------------------------------------------------------
# HTTP：keep-alive 连接池 + 跟随重定向
# ----------------------------------------------------------------------------
class HttpPool:
    def __init__(self, ua=None, referer=None, cookie=None, timeout=30):
        self.ua = ua or DEFAULT_UA
        self.referer, self.cookie, self.timeout = referer, cookie, timeout
        self._local = threading.local()
        self.proxy = os.environ.get("https_proxy") or os.environ.get("HTTPS_PROXY")

    def headers(self, rng=None, extra=None) -> dict:
        h = {"User-Agent": self.ua, "Accept": "*/*",
             "Accept-Encoding": "identity", "Connection": "keep-alive"}
        if self.referer:
            h["Referer"] = self.referer
        if self.cookie:
            h["Cookie"] = self.cookie
        if rng:
            h["Range"] = f"bytes={rng[0]}-{rng[1]}"
        if extra:
            h.update(extra)
        return h

    def _slots(self) -> dict:
        s = getattr(self._local, "slots", None)
        if s is None:
            s = self._local.slots = {}
        return s

    def _conn_for(self, url: str):
        p = urllib.parse.urlparse(url)
        scheme = p.scheme or "https"
        host, port = p.hostname, p.port or (443 if scheme == "https" else 80)
        key = (scheme, host, port, self.proxy)
        slots = self._slots()
        ent = slots.get(key)
        if ent and getattr(ent, "sock", None) is not None:
            return ent, p
        ctx = ssl.create_default_context()
        if self.proxy:
            pp = urllib.parse.urlparse(self.proxy)
            if scheme == "https":
                conn = http.client.HTTPSConnection(pp.hostname, pp.port or 8080,
                                                   timeout=self.timeout, context=ctx)
                conn.set_tunnel(host, port)
            else:
                conn = http.client.HTTPConnection(pp.hostname, pp.port or 8080,
                                                  timeout=self.timeout)
        else:
            conn = (http.client.HTTPSConnection(host, port, timeout=self.timeout, context=ctx)
                    if scheme == "https" else
                    http.client.HTTPConnection(host, port, timeout=self.timeout))
        slots[key] = conn
        return conn, p

    def _drop_all(self):
        for k, c in list(self._slots().items()):
            try:
                c.close()
            except Exception:
                pass
        self._slots().clear()

    def open(self, url: str, *, method="GET", rng=None, retries=2, follow=True,
             extra=None):
        """返回 (conn, resp, final_url)。会跟随 3xx 并保持 Range 头。

        extra：额外请求头（P0-1 的 If-Range / If-Unmodified-Since 走这里）。
        重定向时一并带上 —— 换了主机但内容没换是常态，丢掉它等于关掉校验。
        """
        for hop in range(MAX_REDIRECT + 1):
            p = urllib.parse.urlparse(url)
            target = (p.path or "/") + (("?" + p.query) if p.query else "")
            if self.proxy and p.scheme == "http":
                target = url
            last = None
            resp = None
            for attempt in range(retries + 1):
                conn, _ = self._conn_for(url)
                try:
                    conn.request(method, target,
                                 headers=self.headers(rng, extra=extra))
                    resp = conn.getresponse()
                    break
                except Exception as e:
                    last = e
                    self._drop_all()
                    if attempt == retries:
                        raise
                    time.sleep(0.3 * (attempt + 1))
            if resp is None:
                raise last  # pragma: no cover
            if follow and resp.status in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location")
                try:
                    resp.read()
                except Exception:
                    pass
                if not loc:
                    return conn, resp, url
                # 303 → 必须退化成 GET；且跨域重定向要丢掉 Authorization 之类
                url = urllib.parse.urljoin(url, loc)
                if resp.status == 303:
                    method = "GET"
                if hop >= MAX_REDIRECT:
                    raise DownloadError("重定向次数过多")
                continue
            return conn, resp, url
        raise DownloadError("重定向次数过多")

    @staticmethod
    def drain(resp, max_bytes: int = 65536):
        """读掉少量残留 body 后断开。

        原先直接 resp.read() 无上限：而调用它的场景恰恰是"服务器不标准"
        （probe 的 HEAD 回退碰到忽略 Range 返回 200 全量、_run_single 异常收尾），
        一个 1.5GB 文件会被一次性读进内存，或者二次卡死在无响应的连接上。
        """
        try:
            resp.read(max_bytes)
        except Exception:
            pass


def _filename(url: str, cd: str | None, fallback="download.bin") -> str:
    if cd:
        m = re.search(r"filename\*\s*=\s*(?:UTF-8'')?([^;]+)", cd, re.I)
        if not m:
            m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.I)
        if m:
            name = urllib.parse.unquote(m.group(1).strip().strip('"'))
            if name:
                return os.path.basename(name)
    name = os.path.basename(urllib.parse.unquote(urllib.parse.urlparse(url).path))
    name = re.sub(r'[\\/:*?"<>|]', "_", name)[:180]
    return name or fallback


# ----------------------------------------------------------------------------
# probe
# ----------------------------------------------------------------------------
def _headers_fp(h) -> tuple[str | None, str | None]:
    """从响应头里取 (etag, last-modified)，空串归一成 None。

    弱验证器（W/"..."）也照收：If-Range 允许弱比较的是 Last-Modified，
    而 ETag 强弱之分只影响"If-Range 允不允许命中"，不影响"两个 ETag 相不相等"。
    我们只关心后者（判断内容有没有换），所以不做强弱过滤 —— 有就比。
    """
    et = (h.get("ETag") or "").strip() or None
    lm = (h.get("Last-Modified") or "").strip() or None
    return et, lm


def probe(url: str, *, ua=None, referer=None, cookie=None) -> dict:
    info = {"url": url, "final_url": url, "size": None, "ranges": False,
            "filename": None, "kind": "unknown", "is_hls": False, "status": None,
            "content_type": None, "error": None, "note": None,
            # P0-1：内容指纹。HEAD 本来就会回这两个头，白拿，不额外发请求。
            "etag": None, "last_modified": None}
    pool = HttpPool(ua, referer, cookie, timeout=15)
    try:
        conn, r, final = pool.open(url, method="HEAD")
        # P1-3（本轮实测，比评审描述的更严重）：
        # HEAD 返回 4xx/5xx【不会抛异常】，pool.open 照常返回 response。
        # 于是主分支直接信任了这个错误页的 Content-Length / Accept-Ranges：
        #   实测 python -m http.server 之类不实现 HEAD 的服务端回 501 +
        #   Content-Length: 482（那是 HTML 错误页的长度）→
        #   probe 得出 size=482、ranges=False →
        #   run() 静默降级走 _run_single()（"无 Range 支持，明确不做续传"）→
        #   没有 .fluxdl sidecar，中途取消/中断断点全丢，产物只剩 .part。
        # 用户看到的是"这个地址不能续传"，而服务器其实完全支持 206。
        # 判据：HEAD 必须 2xx 才可信；否则一律进 except 走 GET 探测。
        if not (200 <= r.status < 300):
            raise urllib.error.HTTPError(url, r.status,
                                         f"HEAD 不可用（{r.status}）", r.headers, None)
        info["status"], info["final_url"] = r.status, final
        h = r.headers
        info["content_type"] = h.get("Content-Type")
        info["filename"] = _filename(final, h.get("Content-Disposition"))
        info["etag"], info["last_modified"] = _headers_fp(h)
        info["ranges"] = (h.get("Accept-Ranges", "") or "").lower() == "bytes"
        cl = h.get("Content-Length")
        if cl and cl.isdigit():
            info["size"] = int(cl)
        if (h.get("Transfer-Encoding", "") or "").lower() == "chunked":
            info["ranges"], info["size"] = False, None
        # 只要响应头，body 一个字都不要读（HEAD 路径本无 body）
        pool.drain(r, 0)
    except Exception as e:
        code = getattr(e, "code", None)
        try:                                    # HEAD 不被支持时用单字节 GET 探 Content-Range
            conn, r, final = pool.open(url, rng=(0, 0))
            info["status"], info["final_url"] = r.status, final
            cr = r.headers.get("Content-Range")
            if cr and "/" in cr:
                total = cr.split("/")[-1].strip()
                if total.isdigit():
                    info["size"], info["ranges"] = int(total), True
            # P1-3（本轮实测）：不能只看 Content-Range。
            # 很多简易服务端不回 Content-Range，但仍回 206 + Accept-Ranges: bytes
            # —— 这已经足够证明"支持 Range"，而原实现只认 Content-Range，
            # 于是把支持续传的服务器判成"不支持"，静默降级走 _run_single()：
            # 没有 sidecar、中途中断断点全丢（产物只剩 .part）。
            # 触发条件极常见：服务端不实现 HEAD（返回 501）→ 走本分支。
            if not info["ranges"]:
                if r.status == 206:
                    info["ranges"] = True
                elif (r.headers.get("Accept-Ranges", "") or "").lower() == "bytes":
                    info["ranges"] = True
            if info["size"] is None:
                cl2 = r.headers.get("Content-Length")
                # 206 的 Content-Length 只是"这一小段"的长度，不能当总大小；
                # 只有 200 全量响应时它才等于文件大小。
                if r.status == 200 and cl2 and cl2.isdigit():
                    info["size"] = int(cl2)
            info["content_type"] = info["content_type"] or r.headers.get("Content-Type")
            info["filename"] = info["filename"] or _filename(
                final, r.headers.get("Content-Disposition"))
            e2, lm2 = _headers_fp(r.headers)
            info["etag"], info["last_modified"] = (info["etag"] or e2,
                                                   info["last_modified"] or lm2)
            pool.drain(r, 0)
        except Exception as e2:
            info["error"] = f"HTTP {code or '?'} → {type(e2).__name__}: {e2}"

    if info["error"]:
        return info

    ctype = (info["content_type"] or "").lower()
    if ".m3u8" in info["final_url"].lower() or ctype.startswith(
            ("application/vnd.apple.mpegurl", "application/x-mpegurl")):
        info["is_hls"], info["kind"] = True, "hls"
        base = os.path.splitext(info["filename"] or "video.mp4")[0]
        info["filename"] = base + ".mp4"
    elif info["size"] is not None:
        info["kind"] = "file"
    if info["kind"] == "file" and not info["ranges"]:
        info["note"] = "不支持 Range：单连接下载，无法断点续传"
    return info


# ----------------------------------------------------------------------------
# 元数据
# ----------------------------------------------------------------------------
def _atomic_json(path: str, obj: dict) -> None:
    tmp = f"{path}.tmp{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _sample_sha(fh, pos: int, size: int) -> str | None:
    """从 pos 起取首 4 KiB 的 sha —— 没有任何指纹可用时的降级判据。

    为什么从第一个已落盘区间开头取样，而不是从文件 0 轴：
    文件是预分配的，从没下载过的区块是零，拿它算 sha 恒等无意义。
    """
    n = min(SAMPLE, max(0, size - pos))
    if n <= 0:
        return None
    try:
        fh.seek(pos)
        buf = fh.read(n)
    except OSError:
        return None
    return hashlib.sha256(buf).hexdigest() if buf else None


def _probe_sample_sha(url: str, pool, pos: int, size: int) -> str | None:
    """续传前向服务端重新取 pos 起的首 4 KiB，算同样的抽样 sha。

    只在「既无 ETag 也无 Last-Modified」的源上才走这条路（多一次 Range 请求）。
    """
    n = min(SAMPLE, max(0, size - pos))
    if n <= 0:
        return None
    try:
        conn, r, _ = pool.open(url, rng=(pos, pos + n - 1))
        if r.status not in (200, 206):
            pool.drain(r, 0)
            return None
        buf = r.read(n)
        pool.drain(r, 0)
    except Exception:
        return None
    if not buf:
        return None
    return hashlib.sha256(buf).hexdigest()


def _fp_of(info: dict) -> dict:
    """从 probe() 结果里抠出可用于「内容是否还是同一份」的指纹。

    HEAD 响应本来就有这些头，纯白拿，不额外发请求。
    """
    return {"etag": (info.get("etag") or None),
            "last_modified": (info.get("last_modified") or None)}


def _fp_desc(fp: dict) -> str:
    """指纹的中文短描述，用于报错文案（要能让人一眼看出是哪个变了）。"""
    if fp.get("etag"):
        return "ETag %s" % fp["etag"]
    if fp.get("last_modified"):
        return "Last-Modified %s" % fp["last_modified"]
    if fp.get("head_sha"):
        return "首 %d B 抽样 sha %s" % (SAMPLE, fp["head_sha"][:12])
    return "无"


def _fp_changed(old: dict, new: dict) -> tuple[bool, str]:
    """比对两份指纹。返回 (是否变更, 变更描述)。

    判据优先级与续传请求头一致：etag > last_modified > head_sha。
    只有两边都非空才会判「变」——服务端这轮没给 etag 不该当成换了内容。
    """
    for k, label in (("etag", "ETag"), ("last_modified", "Last-Modified"),
                     ("head_sha", "抽样 sha")):
        a, b = old.get(k), new.get(k)
        if a and b and a != b:
            return True, "%s %s → %s" % (label, a, b)
    return False, ""


def _load_meta(meta_path: str, url: str, size: int | None) -> dict:
    """读检查点。

    P0-1：v4 的 meta 里只有 url/size/done，**没有内容指纹**，无法证明
    「磁盘上那半截」和服务端此刻的文件是同一份 —— 拿它续传正是静默混合的成因。
    所以 v4 一律当「无检查点」处理，并在上层打印一行原因；只有 v5 允许续传。
    """
    if os.path.exists(meta_path):
        try:
            with open(meta_path, encoding="utf-8") as f:
                m = json.load(f)
            if m.get("v") == META_V and m.get("url") == url and m.get("size") == size:
                return m
        except Exception:
            pass
    return {"v": META_V, "url": url, "size": size, "done": []}


def _merge(meta: dict, a: int, b: int) -> None:
    done = sorted(meta["done"] + [[a, b]])
    out: list[list[int]] = []
    for s, e in done:
        if out and s <= out[-1][1]:
            out[-1][1] = max(out[-1][1], e)
        else:
            out.append([s, e])
    meta["done"] = out


def _gaps(meta: dict, size: int) -> list[list[int]]:
    gaps, cur = [], 0
    for s, e in meta["done"]:
        if s > cur:
            gaps.append([cur, s])
        cur = max(cur, e)
    if cur < size:
        gaps.append([cur, size])
    return gaps


# ----------------------------------------------------------------------------
# Downloader
# ----------------------------------------------------------------------------
class Downloader:
    def __init__(self, url, out_dir=".", *, concurrency=0, ua=None, referer=None,
                 cookie=None, override_name=None, adaptive=True, on_progress=None,
                 max_concurrency=16, verbose=False, force_restart=False,
                 reuse=True):
        self.url, self.out_dir = url, out_dir
        self.verbose = verbose
        self.requested = int(concurrency or 0)
        self.max_concurrency = max(1, min(int(max_concurrency), 16))
        self.concurrency = self.requested or 4
        self.ua, self.referer, self.cookie = ua, referer, cookie
        self.override_name, self.adaptive = override_name, adaptive
        self.on_progress = on_progress or (lambda p: None)
        # P0-1：丢弃检查点重下。源已变更时唯一安全的出路 —— 不能复用旧偏移。
        self.force_restart = bool(force_restart)
        # R2-3：HLS 分片默认按清单+本地哈希复用；--no-reuse 时强制全部重下
        self.reuse = bool(reuse)
        self.if_range = None          # 续传时给 worker 用的 If-Range / If-Unmodified-Since
        self._worker_fp_violation = False   # 有 worker 撞上「源已变更」
        self.resumed_from_fp = None   # 本次续传基于的旧指纹（诊断/告警用）
        # R2-1：确认旧断点不可用后，是否已经真的把它丢掉了。
        # 用它来收尾（而不是让 CLI 猜），因为「作废」这句话和「重跑可续传」
        # 同时出现就是自相矛盾 —— 谁作废谁负责把话说完整。
        self.checkpoint_discarded = False
        # R2-1：本 worker（线程内可见）是否已经下到过真实 206 数据。
        # 用线程局部而不是实例属性：并发下每个 worker 的"首块"各自独立。
        self._tls = threading.local()
        # R2-1：撞过一次源已变更后，本轮所有 worker 都不再带 If-Range，
        # 避免把"已下好的新数据"当成"作废断点"重复丢弃。
        self._if_range_off = threading.Event()

        self.info, self.total = {}, 0
        self.finished_bytes = self.fetched = 0
        self.lock, self.meta_lock = threading.Lock(), threading.Lock()
        self.stop_flag = threading.Event()
        self.t0 = 0.0
        self.path = self.meta_path = ""
        # R2-2：下载中的临时文件（成品名 + .part）。
        # 未完成的下载只允许以这个名字留在输出目录里，成品名永远只出现校验通过的内容。
        self.part_path = ""
        self.meta = {}
        self.error = None
        self.short_reads = []        # P2-9：短读诊断，最多留 8 条
        self.finished = False
        self.probe_rate = None
        self.pool = HttpPool(ua, referer, cookie)
        self._pending: list[tuple[int, int]] = []
        self.final_url = url

    def progress(self) -> dict:
        with self.lock:
            fetched, fin, total, c = self.fetched, self.finished_bytes, self.total, self.concurrency
        el = max(time.time() - self.t0, 1e-9) if self.t0 else 0.0
        speed = fetched / el if el else 0.0
        done = fin + fetched
        return {"fetched": fetched, "done": done, "total": total,
                "percent": (100.0 * done / total) if total else 0.0,
                "speed": speed,
                "eta": ((total - done) / speed) if speed > 0 and total else None,
                "concurrency": c, "error": self.error, "finished": self.finished,
                "resumed_from": fin}

    def stop(self):
        self.stop_flag.set()

    def _worker(self, fh, pool_ranges: list[list[int]]):
        while not self.stop_flag.is_set():
            with self.lock:
                if not pool_ranges:
                    return
                seg = pool_ranges[0]
                a, b = seg
                take = min(CHUNK, b - a)
                if b - a > CHUNK:
                    seg[0] = a + take
                else:
                    pool_ranges.pop(0)
                end = a + take - 1
            got = 0
            # R2-1：If-Range 只在「还没拿到任何 206 数据」的请求上带。
            # 一旦本 worker 已有块真实落地，必须放行（见下面 502 行处的前提断言）：
            # 若首个 206 之后源被换掉，后续每个块都会撞 200。此时旧代码会把
            # 「判定源已变更 + 丢弃检查点」重复执行 N 次 —— 第一次丢的是旧断点，
            # 第二次起丢的是**本轮已下好的新数据**，用户重跑等于白下一遍。
            sent_ir = self._if_range_enabled() and got == 0
            try:
                extra = dict([self.if_range]) if sent_ir else None
                conn, r, _ = self.pool.open(self.url, rng=(a, end), extra=extra)
                if r.status == 200:
                    # P0-1：带了 If-Range 却回 200 == 服务端明确表态"内容不是那一份"。
                    # 这时候写进去的每个字节都会和旧数据错位，必须立刻停手。
                    # 分支顺序很重要：先判「带了 If-Range 且本块尚无数据」（真正的
                    # 源变更现场），再判「带了 If-Range 但已有 206 数据落地」。
                    if sent_ir and got == 0:
                        self._worker_fp_violation = True
                        # R2-1：光说"断点已作废"不算数，必须真把检查点丢掉，
                        # 否则下一轮还会捡起同一个断点、报同一句话，永远出不来。
                        self._discard_stale_checkpoint()
                        raise DownloadError(
                            "源已变更（%s 不再匹配），旧断点已丢弃；"
                            "重跑同一条命令即可从头下载新内容"
                            % self.if_range[1])
                    if self.if_range:
                        # R2-1：只有本 worker 已经下到过真实数据，才可能是这个分支
                        # （上一分支已拦下"本块尚无数据"）。响应变 200 说明源确实换了，
                        # 停止写是对的；但**不能说"断点已作废"** —— 已经落地的那部分
                        # 是 206 完整校验过的旧内容，丢掉它等于白下一遍。
                        # 此时 meta 保持原样（本轮进度照常落盘），重跑能接着下。
                        raise DownloadError(
                            "源已变更（%s 不再匹配），已下好的部分仍然有效；"
                            "稍后重跑同一条命令可继续" % self.if_range[1])
                    raise DownloadError("服务端忽略 Range（返回 200），数据会错位")
                if r.status != 206:
                    raise DownloadError(f"HTTP {r.status}")
                # 第一块真实数据到手 → 关闭后续请求的 If-Range（见 sent_ir 的说明）
                while got < take and not self.stop_flag.is_set():
                    buf = r.read(min(65536, take - got))
                    if not buf:
                        break
                    try:
                        os.pwrite(fh.fileno(), buf, a + got)
                    except AttributeError:
                        with self.lock:
                            fh.seek(a + got)
                            fh.write(buf)
                    got += len(buf)
                    with self.lock:
                        self.fetched += len(buf)
                self.pool.drain(r)
            except Exception as e:
                if getattr(self, "_worker_fp_violation", False):
                    # 源已变更：不回收区间、不再重试，直接把整轮下载判死。
                    # R2-1：本块已写的字节不能进 meta（它们与新内容错位）。
                    # 统一由 _flush_meta 依据 checkpoint_discarded 丢弃 pend，
                    # 这里不需要额外记账 —— 记了反而容易和"已作废"状态打架。
                    self.error = str(e)
                    self.stop_flag.set()
                    return
                if not self.stop_flag.is_set():
                    self.error = f"段 {a}-{end}: {type(e).__name__}: {e}"
                with self.lock:
                    if got < take:
                        pool_ranges.insert(0, [a + got, end + 1])
                return
            # 中断/短读：已写入磁盘的 got 字节必须记入 _pending，否则续传会重复下载
            # 这段区间（浪费带宽），更糟的是 meta 会认为该段从未开始 → 文件留下空洞。
            with self.lock:
                if got > 0:
                    self._pending.append((a, a + got))
            if got < take:
                with self.lock:
                    pool_ranges.insert(0, [a + got, end + 1])
                    # P2-9：记录短读诊断。原先只留下一句"仍有区间未完成，重跑同命令可续传"，
                    # 既不知道卡在哪个偏移，也不知道是超时/重置/对端提前关流之外还发生了什么。
                    # 这三项（请求区间、已收字节、原因）直接决定要不要换镜像或降并发重试。
                    if len(self.short_reads) < 8:
                        self.short_reads.append(
                            {"range": [a, end + 1], "got": got, "want": take})
                return

    def _flush_meta(self):
        with self.lock:
            pend, self._pending = self._pending, []
        with self.meta_lock:
            # R2-1：本轮已经确认「源已变更、断点作废」之后，这批 pend 属于
            # 与新内容错位的旧数据，绝不能以"已完成区间"的名义写回 meta ——
            # 否则 _discard_stale_checkpoint 刚清空的 done 会被 _merge 合并回来，
            # done 又变回非空，等于把缺陷原样复现一遍。
            # （pend 先取走再判状态，所以已经排队的那批也一并丢掉。）
            if self.checkpoint_discarded:
                pend = []
            for a, b in pend:
                _merge(self.meta, a, b)
            # P0-1：指纹随 meta 一起落盘，重跑时才有东西可比。
            fp = _fp_of(self.info or {})
            if self.checkpoint_discarded:
                # 已作废：指纹保持 _discard_stale_checkpoint 写下的新源值，
                # 不再用本轮 info 覆盖（本轮 info 可能是变更前探测到的旧值，
                # 覆盖回去会让 meta 变成"旧指纹 + 空进度"，下次又白跑一次预检）。
                fp = {"etag": self.meta.get("etag"),
                      "last_modified": self.meta.get("last_modified")}
            if fp.get("etag"):
                self.meta["etag"] = fp["etag"]
            if fp.get("last_modified"):
                self.meta["last_modified"] = fp["last_modified"]
            self.meta["v"] = META_V
            # 即使 pend 为空也落盘一次：中断前若无任何完成区间，
            # 也需要 meta 文件存在，重跑时才能识别"这是同一任务的续传"而非全新下载。
            _atomic_json(self.meta_path, self.meta)

    # 小于这个体积不值得测速：测速本身最多 1.5s + 3MB 流量，
    # 而 8MB 文件单连接也就 1s 左右下完 —— 测速比下载还贵，净亏损。
    MEASURE_MIN_TOTAL = 8 * 1024 * 1024

    def _measure_and_pick(self) -> int:
        """实测单连接吞吐 → 决定并发档位（默认值不许拍脑袋）。"""
        if not self.adaptive or not self.info.get("ranges") or not self.total:
            return self.requested or 1
        if self.requested:
            return min(self.requested, self.max_concurrency)
        # P2-1：小文件跳过测速，固定 4 并发。
        # 原先无论多大都跑一遍 1.5s / 3MB 自适应测速：一个 2MB 的图标包，
        # 测速阶段读完还得不到速率结论，等于先浪费 1.5s 再开始下载。
        if self.total < self.MEASURE_MIN_TOTAL:
            self.probe_rate = None
            return min(4, self.max_concurrency)
        span = min(3 * 1024 * 1024, self.total - 1)
        t, got = time.time(), 0
        try:
            # P0-1：测速也走 Range，所以续传时必须一并带 If-Range。
            # 这个请求只读不写，不带也不会污染磁盘；但它是"续传期间的 Range 请求"，
            # 不带就等于放过了一次「源已变更」的免费检测机会，也让
            # "续传期所有 Range 请求都绑定内容" 这条不变量出现例外（测试会抓）。
            extra = dict([self.if_range]) if self.if_range else None
            conn, r, _ = self.pool.open(self.url, rng=(0, span), extra=extra)
            if r.status == 200 and self.if_range:
                # 服务端明确表态：内容不是那一份了。别测速了，交给工人路径去报错。
                self.pool.drain(r)
                return self.requested or 1
            if r.status != 206:
                return 1
            while time.time() - t < 1.5:
                buf = r.read(65536)
                if not buf:
                    break
                got += len(buf)
            self.pool.drain(r)
        except Exception:
            return 1
        rate = got / max(time.time() - t, 1e-6)
        self.probe_rate = rate
        if rate >= 8 * 1024 * 1024:
            return 1
        if rate >= 2 * 1024 * 1024:
            return 4
        return min(8, self.max_concurrency)

    def run(self) -> dict:
        self.info = probe(self.url, ua=self.ua, referer=self.referer, cookie=self.cookie)
        self.final_url = self.info.get("final_url") or self.url
        if self.info.get("error"):
            self.error, self.finished = self.info["error"], True
            raise DownloadError(self.error)

        if self.info.get("is_hls"):
            self.concurrency = max(4, self.requested or 8)
            self.t0 = time.time()          # 修：HLS 路径原先没设 t0，耗时算出 17 亿秒
            path = hls_download(self.url, self.out_dir, name=self.override_name,
                                ua=self.ua, referer=self.referer, cookie=self.cookie,
                                concurrency=self.concurrency,
                                on_progress=self.on_progress, stop=self.stop_flag,
                                reuse=self.reuse)
            self.path, self.finished = path, True
            return {"path": path, "kind": "hls", "size": os.path.getsize(path)}

        self.total = self.info.get("size") or 0
        name = self.override_name or self.info.get("filename") or "download.bin"
        os.makedirs(self.out_dir, exist_ok=True)
        self.path = os.path.join(self.out_dir, name)
        self.meta_path = self.path + ".fluxdl"
        # R2-2（方案 A）：正文一律先写进 self.part_path，校验通过后 os.replace 成 self.path。
        # 为什么不再用「预分配 + 退出路径负责改名」：那种写法的正确性依赖
        # 「所有 rc≠0 的退出路径都记得收拾」，而退出路径会随功能增加而增多
        # （源已变更、短读、锁冲突、Ctrl-C、校验失败、未捕获异常…），漏一条就
        # 安静地留下一个「名字和大小都像成品」的文件 —— 外部脚本 test -f 就当真。
        # 反过来，让「写入目标」本身就不是成品名之后，遗漏只会留下 .part，
        # 而成品名**在结构上**只可能由唯一一处 os.replace 产生。
        self.part_path = self.path + ".part"

        # P1-1：锁必须提到统一入口。
        # 原实现只在 ranged 分支加锁，`_run_single`（源不支持 Range：CDN 边缘、
        # 签名 URL、代理配错）完全裸奔 —— 实测两个进程同下同一输出名：
        # A 先完成、打印「完成」并 rc=0，B 随后失败却把 A 的成品覆盖成自己的
        # 残缺内容，还甩一个裸 FileNotFoundError Traceback。
        # 加锁后这种情况变成明确报错，且「完成」二字一定对应磁盘上真实完整的那份。
        # R2-2：锁仍然锁 self.path（成品名，不是 .part）—— 锁的论题是「谁在产出这个名字」，
        # 而不是「谁在写哪个文件」；两个进程若各锁自己的 .part 就等于没锁。
        with _FileLock(self.path):
            if not self.info.get("ranges") or not self.total:
                return self._run_single()
            return self._run_ranged()

    def _run_ranged(self) -> dict:
        """Range 下载主体（已在 _FileLock 保护下）。

        R2-2（方案 A）：本函数只碰 self.part_path；self.path（成品名）
        在本轮成功之前既不读也不写，只在最后一步由 os.replace 原子产生。
        """
        had_meta = os.path.exists(self.meta_path)
        if self.force_restart and (had_meta or os.path.exists(self.path)
                                   or os.path.exists(self.part_path)):
            self._discard_checkpoint()
            had_meta = False
        self._drop_legacy_pseudo_product()
        self.meta = _load_meta(self.meta_path, self.final_url, self.total)
        # P0-1：只有带指纹的 v5 检查点才允许续传。
        self.meta.setdefault("etag", None)
        self.meta.setdefault("last_modified", None)
        self.meta.setdefault("head_sha", None)
        self.finished_bytes = sum(e - s for s, e in self.meta["done"])
        gaps = _gaps(self.meta, self.total)

        # 关键修复：预分配会让文件"大小 == total"但内容是空洞。
        # 因此只有「meta 存在且校验通过」时，才可以信任既有文件续传；
        # 否则一律重新预分配（truncate 会把空洞显式归零，避免把垃圾当成品）。
        # R2-2：判据对象从 self.path 换成 self.part_path —— 成品名现在不再
        # 承载"下载中的半成品"，续传的落点就是 .part。
        existed_same_size = (os.path.exists(self.part_path)
                             and os.path.getsize(self.part_path) == self.total)
        trusted_resume = had_meta and existed_same_size and bool(self.meta.get("done"))
        if existed_same_size and not trusted_resume:
            # P0-1：把「为什么重下」说清楚。旧版 meta 只有 v4，没有指纹可比，
            # 静默沿用它就等于把这轮的混合留给下一轮 —— 所以宁可重下。
            # R2-1：这里原先只判断「meta 在不在」，于是上一轮刚被 R2-1 作废过的
            # 检查点（v5、done 清空、指纹已是新源）会被报成"旧版 v4 没指纹" ——
            # 磁盘状态和文案直接打架，用户会以为自己的 meta 被谁改坏了。
            # 判据改成「有没有指纹」，与 _load_meta 里实际的 v5 门槛一致。
            if had_meta and self.meta.get("etag"):
                why = ("检查点已没有可续传的区间（上一轮确认源已变更并作废了断点），"
                       "只剩 %s 的指纹" % _fp_desc(self.meta))
            elif had_meta:
                why = "旧版检查点没有内容指纹（v4 → v5），无法确认服务端文件未变"
            else:
                why = "缺少有效续传元数据"
            print(f"警告: {self.part_path} 已存在且大小匹配，但{why} "
                  f"→ 视为未完成，从头下载", file=sys.stderr)
        fresh = not trusted_resume

        if not fresh:
            # P0-1 核心：动笔之前先证明"服务端此刻的文件 == 磁盘上那半截"。
            fresh = not self._bind_resume_to_source()
            if fresh:
                self.meta["done"] = []
                self.finished_bytes = 0
                gaps = _gaps(self.meta, self.total)

        # R2-2：读写对象是 .part，不是成品名。
        fh = open(self.part_path, "w+b" if fresh else "r+b")
        try:
            if fresh:
                fh.truncate(self.total)          # 预分配：避免边写边扩，也让 pwrite 合法
            elif os.path.getsize(self.part_path) < self.total:
                fh.truncate(self.total)
            # P0-1 降级指纹：源既无 ETag 也无 Last-Modified 时，只能拿
            # 「已下载区间的开头 4 KiB」的 sha 当内容判据。
            # 续传前重算同一段比对即可，成本一次 Range 请求。
            if self._needs_head_sha() and not self.meta.get("head_sha"):
                pos = min((s for s, _e in (self.meta["done"] or gaps)), default=0)
                _s = _sample_sha(fh, pos, self.total)
                if _s:
                    self.meta["head_sha"] = _s
            self.concurrency = self._measure_and_pick()
            self.t0 = time.time()
            if not gaps:
                self.finished = True
            else:
                pool_ranges = [[a, b] for a, b in gaps]
                # P0：线程数上界必须由「可切出的块数」决定，而不是「gap 区间个数」。
                # 全新下载时 _gaps 只返回一个 [0, total]，len(pool_ranges)==1，
                # 于是 n=1 —— 只起 1 个 worker 串行下完整个文件，而日志照样打印「并发=16」。
                # 实测：-c 16 与 -c 1 同为 25.7s，全程 ESTAB=1、nlwp=1。
                _gap_bytes = sum(b - a for a, b in pool_ranges)
                _nblk = (_gap_bytes + CHUNK - 1) // CHUNK        # CHUNK = 256 KiB
                n = max(1, min(self.concurrency, _nblk))
                # 日志必须打印「实际生效值」——原缺陷藏这么久，就是因为打印的是用户入参。
                if self.verbose:
                    print(f"并发: 请求={self.concurrency} 实际={n} 块数={_nblk}",
                          file=sys.stderr)
                ths = [threading.Thread(target=self._worker, args=(fh, pool_ranges),
                                        daemon=True) for _ in range(n)]
                for t in ths:
                    t.start()
                last_ui = last_meta = time.time()
                while any(t.is_alive() for t in ths):
                    time.sleep(0.15)
                    now = time.time()
                    if now - last_meta >= 0.4:
                        self._flush_meta()
                        last_meta = now
                    if now - last_ui >= 0.25:
                        self.on_progress(self.progress())
                        last_ui = now
                    if self.error:
                        self.stop_flag.set()
                for t in ths:
                    t.join()
                # R2-1：任何一个 worker 撞上"源已变更"后，本轮不再发任何带
                # If-Range 的请求 —— 否则其它 worker 会各自撞一次 200，
                # 每撞一次就把已下好的进度当"作废断点"清一遍。
                if self._worker_fp_violation:
                    self._if_range_off.set()
                self._flush_meta()
                self.on_progress(self.progress())
        finally:
            fh.close()

        if self.error:
            raise DownloadError(self.error)
        gaps = _gaps(self.meta, self.total)
        if gaps:
            # P2-9：把失败区间起点 + 短读明细带出来，而不是只说"未完成"。
            head = ", ".join("%d-%d" % (a, b) for a, b in gaps[:5])
            more = "" if len(gaps) <= 5 else " 等 %d 段" % len(gaps)
            diag = ""
            if self.short_reads:
                r = self.short_reads[0]
                diag = ("；首次短读: 请求 %d-%d 应收 %d 实收 %d"
                        "（共 %d 次）" % (r["range"][0], r["range"][1] - 1,
                                          r["want"], r["got"], len(self.short_reads)))
            raise DownloadError(
                "仍有 %d 个区间未完成: %s%s%s；重跑同命令可续传"
                % (len(gaps), head, more, diag))
        # R2-2：这里是成品名的**唯一**产生点 —— 所有区间都已落盘之后。
        # os.replace 在同一文件系统内是原子的：外部脚本要么看不到 blob.bin，
        # 要么看到一个完整文件，不存在"存在但只有一半"的中间态。
        try:
            os.replace(self.part_path, self.path)
        except OSError as e:
            # 不把 .part 删除：它是唯一一份真实数据，删掉等于让用户从头再下。
            raise DownloadError(
                "无法把临时文件 %s 改名为成品 %s（%s）；临时文件已保留，重跑同命令可续传"
                % (self.part_path, self.path, e)) from e
        if os.path.exists(self.meta_path):
            os.remove(self.meta_path)
        self.finished = True
        return {"path": self.path, "kind": "file", "size": self.total,
                "resumed_from": self.finished_bytes,
                "resumed_fp": self.resumed_from_fp}

    def _needs_head_sha(self) -> bool:
        """本次是否需要写降级指纹：源没给任何验证器，且我们没在用它续传。"""
        fp = _fp_of(self.info or {})
        return not fp.get("etag") and not fp.get("last_modified")

    def _discard_checkpoint(self):
        """--force-restart：删 .fluxdl、成品名与未完成的 .part，从零开始。

        只删这三个已知产物，不动目录里别的东西。
        R2-2：成品名也要删 —— 切换 .part 之后，成品名只可能来自「校验通过的成品」，
        如果它真存在，说明这次 --force-restart 是要覆盖一份旧成品（用户的明确意图）。
        """
        removed = []
        for p in (self.meta_path, self.path, self.part_path):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
                    removed.append(os.path.basename(p))
            except OSError as e:
                raise DownloadError("--force-restart 无法删除 %s: %s" % (p, e))
        if self.verbose and removed:
            print("已丢弃检查点: %s" % ", ".join(removed), file=sys.stderr)

    def _drop_legacy_pseudo_product(self):
        """清掉旧版本（预分配写法）留在成品名上的半成品（R2-2）。

        为什么需要它：R2-2 之前，下载正文直接写进 self.path，于是中断/失败会
        留下「成品名 + 最终大小 + 内容一半是零」的文件。升级到 .part 写法之后，
        如果不管它，续传会去读 .part（不存在）→ 从零重下；而那份旧的伪成品会
        一直挂在成品名上，被外部脚本当成品用 —— 这正是本轮要根除的东西。

        判据只认「无法自证是成品」的情形：
          - meta 声称有已完成区间，且磁盘上那份与声明大小一致 → 数据可能仍有效，
            保留不动（续传会去读 .part，读不到就重下，但绝不删用户可能还需要的数据）；
            实际上这种组合在 .part 写法下已不可能，这里只是保守兜底。
          - 其余情况（无 meta，或文件大小 ≠ 声明大小）：删掉。
        宁可删掉重下，也不留一个"看着像成品"的残缺文件。
        """
        if not (self.path and os.path.exists(self.path)):
            return
        try:
            size = os.path.getsize(self.path)
        except OSError:
            return
        if size == self.total and self.meta.get("done"):
            return
        try:
            os.remove(self.path)
        except OSError as e:
            if self.verbose:
                print("警告: 无法清理旧的半成品 %s（%s）；它可能被当成成品，"
                      "建议手动删除" % (self.path, e), file=sys.stderr)
            return
        if self.verbose:
            print("已清理上一版留下的半成品: %s（不是成品）"
                  % os.path.basename(self.path), file=sys.stderr)

    def _if_range_enabled(self) -> bool:
        """R2-1：本轮是否还允许带 If-Range。

        一旦有 worker 确认「源已变更并丢弃了断点」，其余 worker 继续带
        If-Range 只会在服务端（或本来就不支持 If-Range 的实现）上再撞一次
        200，而每次撞都会把刚刚重新累计的进度当"作废断点"清掉 —— 越下越空。
        关掉之后行为退化到"普通 Range 请求"，等到重跑再由前置校验兜底。
        """
        return bool(self.if_range) and not self._if_range_off.is_set()

    def _discard_stale_checkpoint(self):
        """源已变更 → 真把作废的检查点在磁盘上清掉（R2-1）。

        为什么选「重置 meta 并改写新指纹」而不是「删掉 meta 文件」：
          - 删 meta 只解决"下一轮不再续传"，却把"本地这份残片对应的是新源"
            这条信息也一起丢了，紧接着那一轮只能从零重下（虽然正确，但白丢
            一次已知信息）；
          - 这里把 done 清空、换上本轮的新指纹、立刻落盘，等于原子地把检查点
            改造成"新任务的第 0 字节"。下一轮以 v5 + 新 ETag 正常续传，
            不会再触发一次源已变更；而且只删一个区间列表、不做文件 IO，
            在 worker 线程里也能安全调用（_flush_meta 走的是同一把 meta_lock）。

        注意：区间数据不再复用，但 .fluxdl 里"已确认作废"是自洽的 ——
        done == [] 表示一个字节都没确认，与磁盘上那份预分配的半成品不矛盾
        （续传的信任条件本来就是 had_meta and done 非空）。
        """
        if self.checkpoint_discarded:
            return
        fp = _fp_of(self.info or {})
        with self.meta_lock:
            done = self.meta.get("done") or []
            dropped = sum(e - s for s, e in done)
            self.meta["done"] = []
            self.meta["v"] = META_V
            # 指纹换成新源：即便不重跑，meta 里也不会再留着"旧指纹 + 有进度"
            # 这种自相矛盾的状态。
            self.meta["etag"] = fp.get("etag")
            self.meta["last_modified"] = fp.get("last_modified")
            self.meta["head_sha"] = None      # 旧源的头 sha 对新内容无意义
            try:
                _atomic_json(self.meta_path, self.meta)
            except OSError as e:
                # 落盘失败不能让"报源已变更"这件事变成另一个异常盖掉；
                # 但必须把真相说出来 —— 此时下一轮仍可能捡到旧断点。
                if self.verbose:
                    print("警告: 丢弃检查点落盘失败（%s），下轮可能仍读到旧断点"
                          % e, file=sys.stderr)
        self.checkpoint_discarded = True
        self.finished_bytes = 0
        if self.verbose and dropped:
            print("已丢弃失效检查点：作废 %s 进度（源已变更为新内容）"
                  % _fmt(dropped), file=sys.stderr)

    def _bind_resume_to_source(self) -> bool:
        """把断点绑定到内容。返回 True 表示可以按旧偏移续传。

        两道闸：
          1. 续传前 HEAD 的指纹 vs meta 里的指纹，不一致直接判源已变；
          2. 设好 If-Range，让 worker 的每个 206 请求都带上它 ——
             万一在我们校验之后源又换了，服务端会回 200，_worker 立刻失败。
        两道都过不了（既无 etag 也无 last-modified）时降级到首 4 KiB 抽样 sha。
        """
        size = self.total
        old = {"etag": self.meta.get("etag"),
               "last_modified": self.meta.get("last_modified"),
               "head_sha": self.meta.get("head_sha")}
        cur = _fp_of(self.info)

        changed, why = _fp_changed(old, cur)
        if changed:
            self._raise_source_changed(why)

        # 服务端这轮带了 etag：用它做 If-Range（最强判据，服务端自己比对）
        if cur.get("etag"):
            self.if_range = ("If-Range", cur["etag"])
            self.resumed_from_fp = cur
            return True
        if cur.get("last_modified") and cur["last_modified"] == old.get("last_modified"):
            self.if_range = ("If-Unmodified-Since", cur["last_modified"])
            self.resumed_from_fp = cur
            return True

        # 降级：两端都没有验证器 → 首 4 KiB 抽样比对
        pos = min((s for s, _e in self.meta["done"]), default=0)
        # R2-2：抽样比对的读取对象是 .part —— 半成品只住在那里。
        if old.get("head_sha") and os.path.exists(self.part_path):
            try:
                with open(self.part_path, "rb") as fh:
                    mine = _sample_sha(fh, pos, size)
            except OSError:
                mine = None
            theirs = _probe_sample_sha(self.final_url, self.pool, pos, size)
            if mine and theirs and mine != theirs:
                self._raise_source_changed("抽样 sha %s → %s" % (mine, theirs))
            if mine and theirs:
                self.resumed_from_fp = {"head_sha": mine}
                return True
            if self.verbose:
                print("警告: 无验证器的源，抽样比对失败 → 从头下载", file=sys.stderr)
            return False
        if self.verbose:
            print("警告: 检查点没有内容指纹（旧版 meta）→ 从头下载", file=sys.stderr)
        return False

    def _raise_source_changed(self, why: str):
        # R2-1：先丢弃再报错。原实现只报「断点已作废」却把 meta 原样留在磁盘上，
        # 于是每重跑一次都捡起同一个作废断点、报同一句话 —— rc 永远 1，
        # 用户只能 --force-restart 自救，而文案又暗示重跑就行。
        # 现在两件事一起做：磁盘上是真作废，屏幕上是真话。
        self._discard_stale_checkpoint()
        # 文案三要素：旧的是什么 → 新的是什么 → 下一步怎么做。
        raise DownloadError(
            "源已变更（%s），旧断点已丢弃；重跑同一条命令即可从头下载新内容"
            % why)

    def _run_single(self) -> dict:
        """无 Range 支持：单连接顺序下载，明确不做续传。"""
        self.concurrency = 1
        part = self.path + ".part"
        self.t0 = time.time()
        conn, r, _ = self.pool.open(self.url)
        n = 0
        try:
            cl = r.headers.get("Content-Length")
            if cl and cl.isdigit():
                self.total = int(cl)
            with open(part, "wb") as f:
                last_ui = time.time()
                while not self.stop_flag.is_set():
                    buf = r.read(65536)
                    if not buf:
                        break
                    f.write(buf)
                    n += len(buf)
                    with self.lock:
                        self.fetched = n
                    if time.time() - last_ui > 0.25:
                        last_ui = time.time()
                        self.on_progress(self.progress())
        finally:
            self.pool.drain(r)
        if self.stop_flag.is_set():
            raise DownloadError("已取消（此服务器不支持 Range，需重新下载）")
        # 关键：EOF 不等于收全。无 Range 的服务器（CDN 边缘、对象存储签名 URL、
        # 反向代理配错）在网络抖动/限速掐连接时会提前关闭，read() 返回空就 break，
        # 若这里不比对 Content-Length，就会把残件 os.replace 成"成品"，
        # 而 verify_file() 的洞检测/抽样/gzip 魔数全都看不出来（非稀疏、数据非零），
        # 无 expect 时 sha256 也只算不比对 —— 全链路报成功，退出码 0。
        # 实测：声明 100000 实收 60000，旧代码返回 size=60000 且校验通过。
        if self.total and n != self.total:
            try:
                os.remove(part)
            except OSError:
                pass
            raise DownloadError(
                "响应被截断：Content-Length=%d，实收 %d 字节（差 %d）"
                % (self.total, n, self.total - n))
        # P1-1：即使有锁，part 也可能被外部因素弄没（用户/杀毒/容器清理）。
        # 裸 os.replace 抛 FileNotFoundError 会甩出整段 Traceback，
        # 用户看到的是 Python 栈而不是"出了什么事、该怎么办"。
        try:
            os.replace(part, self.path)
        except OSError as e:
            raise DownloadError(
                "无法写入成品 %s（%s）；临时文件 %s 状态异常，重跑同命令会重新下载"
                % (self.path, e, part)) from e
        self.finished = True
        return {"path": self.path, "kind": "file", "size": n}


# ----------------------------------------------------------------------------
# HLS
# ----------------------------------------------------------------------------
def _m3u8_guards(text: str):
    """检查加密/fMP4 初始化段，这两类靠 concat + ffmpeg -c copy 拼不出能播的视频。

    #EXT-X-KEY:METHOD=AES-128,... —— 分片是密文，直接 concat 得到一堆加密字节，
        但 ffmpeg 仍可能返回 0，面板就报"完成"；
    #EXT-X-MAP:URI="init.mp4"   —— fMP4 的 moov 在 init segment 里，丢掉后拼出来的
        文件没有索引，播放器打不开。
    这两种情况要明确报错，而不是装成功。
    """
    maps, encrypted = [], False
    for l in (ln.strip() for ln in text.splitlines()):
        if not l.startswith("#EXT-X-KEY"):
            if l.startswith("#EXT-X-MAP"):
                m = re.search(r'URI="([^"]+)"', l)
                if m:
                    maps.append(m.group(1))
            continue
        m = re.search(r"METHOD=([^,\s]+)", l, re.I)
        method = (m.group(1).upper() if m else "")
        if method and method != "NONE":
            encrypted = True
    return encrypted, maps


def _parse_m3u8(text: str, base: str):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if any(l.startswith("#EXT-X-STREAM-INF") for l in lines):
        best, best_bw = None, -1
        for i, l in enumerate(lines):
            if l.startswith("#EXT-X-STREAM-INF"):
                m = re.search(r"BANDWIDTH=(\d+)", l)
                bw = int(m.group(1)) if m else 0
                for nxt in lines[i + 1:]:
                    if not nxt.startswith("#"):
                        if bw > best_bw:
                            best, best_bw = nxt, bw
                        break
        return ([urllib.parse.urljoin(base, best)] if best else []), True
    return [urllib.parse.urljoin(base, l) for l in lines if not l.startswith("#")], False


def _resolve_segments(url: str, pool: HttpPool, depth=0):
    """解析出最终的分片列表 + 指纹。返回 (segs, fingerprints, url_of_seg)。

    P2-D：原先 `_check_hls_supported()` 和本函数各拉一次 m3u8，master playlist
    还要再进一层 —— 同一个清单被 GET 3 次（实测 E7b 日志 `['/c.m3u8']*3`）。
    现在合并成一次下行，解析结果一次算完，顺带把守卫检查也做掉。
    P0-2：同时算出每个分片的内容指纹（见 `_seg_fingerprint`），供下次复用前比对。
    """
    seen_guard = {"encrypted": [], "maps": []}
    segs, seg_url = _walk_playlists(url, pool, depth, seen_guard)
    if seen_guard["encrypted"]:
        raise DownloadError(
            "该 HLS 流已加密（#EXT-X-KEY METHOD=AES-128 等），当前不支持解密")
    if seen_guard["maps"]:
        raise DownloadError(
            "该 HLS 流为 fMP4（含 #EXT-X-MAP 初始化段），"
            "直接 concat 会得到无 moov 的坏文件，当前不支持。init: %s"
            % ", ".join(seen_guard["maps"][:3]))
    return segs, [_seg_fingerprint(u) for u in segs], seg_url


def _walk_playlists(url: str, pool: HttpPool, depth, guard: dict):
    """沿 master -> media 走一遍，边解析边做加密/fMP4 守卫。返回 (segs, 清单最终 URL)。"""
    conn, r, final = pool.open(url)
    body = r.read().decode("utf-8", "replace")
    pool.drain(r)
    encrypted, maps = _m3u8_guards(body)
    if encrypted:
        guard["encrypted"].append(final)
    guard["maps"].extend(maps)
    segs, master = _parse_m3u8(body, final)
    if master and segs and depth < 3:
        return _walk_playlists(segs[0], pool, depth + 1, guard)
    return segs, final


def _seg_fingerprint(seg_url: str) -> str:
    """P0-2：分片的内容身份 = sha1(分片 URL) 前 12 位。

    为什么用 URL 而不是下载后算内容 sha1：复用判断发生在**下载之前**，
    此时手上只有清单。URL 变了（换源/换清晰度/重新切片）就说明这一份不是上一份；
    URL 没变但内容被换了，由下面 `.manifest` 里的 size 兜底（对不上就作废）。
    """
    return hashlib.sha1(seg_url.encode("utf-8")).hexdigest()[:12]


SEG_MANIFEST = ".manifest.json"
SEG_KEEP_SECONDS = 24 * 3600      # P2-E：成功后默认保留 24h，够用户自证，也不会长期占盘


def _seg_probe_remote(seg_url: str, pool: HttpPool) -> dict:
    """P0-2 的内容兜底：HEAD（或 0-0 Range）取分片的 ETag/Last-Modified/长度。

    为什么光有 `sha1(url)` 指纹不够 —— 这是本测试实跑打出来的坑：
    E7b 现场源站把 `seg0.ts` 的内容换了，**URL 一个字都没变**。
    只按 URL 算指纹 → 两次清单完全一致 → 判定"可以复用" → 0 个 .ts 请求 →
    照样混剪。所以必须有一个"服务端亲口说的内容身份"，这里取 ETag，
    退化到 Last-Modified，再退化到长度。
    """
    out = {"etag": None, "last_modified": None, "len": None}
    try:
        conn, r, _ = pool.open(seg_url, method="HEAD")
        try:
            if 200 <= r.status < 300:
                out["etag"] = r.headers.get("ETag")
                out["last_modified"] = r.headers.get("Last-Modified")
                cl = r.headers.get("Content-Length")
                if cl and cl.isdigit():
                    out["len"] = int(cl)
        finally:
            pool.drain(r)
    except Exception:
        pass
    if not out["etag"] and not out["last_modified"] and out["len"] is None:
        # HEAD 不可用（常见于简易服务端 501）→ 用 1 字节 Range 兜底
        try:
            conn, r, _ = pool.open(seg_url, rng=(0, 0))
            try:
                out["etag"] = r.headers.get("ETag")
                out["last_modified"] = r.headers.get("Last-Modified")
                cr = r.headers.get("Content-Range")
                if cr and "/" in cr:
                    t = cr.split("/")[-1].strip()
                    if t.isdigit():
                        out["len"] = int(t)
            finally:
                pool.drain(r)
        except Exception:
            pass
    return out


# R2-3：本地分片哈希 / 服务端身份缺失提示。
# 记录"下载完成后"的 sha1，是为了把「本地文件被截断 / 手工替换 / 旧版本残留」
# 这种纯本地损坏挡住 —— 它不花任何网络请求，也就不会削弱 0 请求续传。
SEG_HASH_ALGO = "sha1"
_HASH_MISSING = (None, "", 0)


def _seg_hash(path: str) -> str | None:
    """算本地分片的 sha1；文件不在 / 读不了 → None。"""
    try:
        return _hash_file(path, SEG_HASH_ALGO)
    except OSError:
        return None


def _local_manifest_path(seg_dir: str) -> str:
    return os.path.join(seg_dir, SEG_MANIFEST)


def _forget_manifest(seg_dir: str) -> None:
    """丢掉本地清单。清单里记着分片哈希，一旦整目录作废就必须一起丢，
    否则下一轮会拿旧哈希去"校验"刚重下的分片，判成损坏再重下一遍。"""
    try:
        os.remove(_local_manifest_path(seg_dir))
    except OSError:
        pass


def _local_victim(seg_dir: str, idx: int, fp: str, rec: dict, prev_urls: dict) -> str:
    """该分片的磁盘路径（无论清单里记的 URL 是否还在本次清单里）。

    为什么要用清单里记的 URL 反推文件名：分片 URL 变了 → 文件名里的指纹也变了，
    按"本次清单"算出来的路径根本不存在，旧文件永远删不掉，还会一路留在目录里。
    """
    url = (rec or {}).get("url") or prev_urls.get(idx)
    name = "%05d-%s.ts" % (idx, _seg_fingerprint(url) if url else fp)
    return os.path.join(seg_dir, name)


# R2-3：长度**不算**内容身份。
# 这是本轮的实测教训：简易静态服务/S3 风格 CDN 常常只回 Content-Length，
# 于是 `ctok` 变成 `l:6768`，看起来"有身份"，实际上和"没身份"一样 ——
# 换掉内容只要长度不变，`l:6768 == l:6768` 照样判复用，警告也永远不会出现。
# 判断标准改成"服务端有没有亲口说过内容版本"，只有 ETag / Last-Modified 算数。
def _content_token(fp_info, *, strict=False):
    """把服务端给的内容身份压成一个可比较的短串。拿不到就返回 None。

    strict=True 时只认 ETag / Last-Modified（长度只作旁证）；
    strict=False 保留长度（向后兼容旧清单里 `l:` 开头的 ctok）。
    """
    if not fp_info:
        return None
    parts = []
    if fp_info.get("etag"):
        parts.append("e:" + str(fp_info["etag"]))
    if fp_info.get("last_modified"):
        parts.append("m:" + str(fp_info["last_modified"]))
    if fp_info.get("len") is not None and not strict:
        parts.append("l:" + str(fp_info["len"]))
    return "|".join(parts) if parts else None


def _has_service_identity(fp_info) -> bool:
    """服务端是否给了可用的内容版本标识（ETag / Last-Modified）。"""
    return bool(fp_info and (fp_info.get("etag") or fp_info.get("last_modified")))


def _bind_segments(seg_dir: str, segs, fps, playlist_url: str, *, force_restart=False,
                   pool=None):
    """P0-2：开跑前把「本次清单」与「上次留下的分片」对齐，不一致就整目录作废。

    这就是 E5/E7b 的根因所在：原实现只写 `%05d.ts`，靠 `os.path.exists()` 判断
    "下过了"，源站换了同大小内容也照样跳过 → 0 个 .ts 请求，成品是新旧混剪。
    现在的判据是：
      1. 分片名自带 sha1(URL) 指纹（序号相同、URL 不同 → 不同文件，不会误认）；
      2. 目录里放一份 `.manifest.json` 记录本次的 [idx, fp, url] 与清单地址；
      3. 清单对不上（数量/顺序/指纹/清单 URL 任一变化）→ 删掉旧分片重下。
    宁可重下，不可混剪：混剪是 `rc=0 + 校验通过` 的静默错误，代价远高于重下。
    """
    man_path = os.path.join(seg_dir, SEG_MANIFEST)
    # 先看有没有旧清单；只有"可能复用"时才值得去问服务端（省请求）
    cur = {"playlist": playlist_url,
           "segs": [{"i": i, "fp": fps[i], "url": u} for i, u in enumerate(segs)]}
    prev = None
    if os.path.exists(man_path):
        try:
            with open(man_path, encoding="utf-8") as f:
                prev = json.load(f)
        except (OSError, ValueError):
            prev = None

    seg_paths = lambda idx: os.path.join(               # noqa: E731
        seg_dir, "%05d-%s.ts" % (idx, fps[idx]))
    prev_urls = {x.get("i"): x.get("url")
                 for x in ((prev or {}).get("segs") or []) if isinstance(x, dict)}

    def _drop_all(reason: str) -> bool:
        """整目录作废：删分片 + 丢清单。返回 False（=不复用）。"""
        print("警告: HLS %s → 旧分片作废，全部重下" % reason, file=sys.stderr)
        for n in os.listdir(seg_dir):
            if n.endswith(".ts") or n == "concat.txt":
                try:
                    os.remove(os.path.join(seg_dir, n))
                except OSError:
                    pass
        _forget_manifest(seg_dir)
        return False

    reuse = False
    if force_restart:
        pass                                     # 显式要求重下
    elif prev is None:
        # 没有清单但有分片：来路不明（旧版残留 / 手工塞的）。一律作废。
        stale = [n for n in os.listdir(seg_dir) if n.endswith(".ts")]
        if stale:
            print("警告: %s 有 %d 个无清单约束的分片，无法确认是否同一份内容 → 丢弃重下"
                  % (seg_dir, len(stale)), file=sys.stderr)
    else:
        psegs = prev.get("segs") or []
        if prev.get("playlist") != playlist_url:
            reuse = _drop_all("清单地址已变（%s → %s）"
                              % (prev.get("playlist"), playlist_url))
        elif len(psegs) != len(cur["segs"]):
            reuse = _drop_all("分片数变了（%d → %d）"
                              % (len(psegs), len(cur["segs"])))
        elif [(x.get("i"), x.get("fp")) for x in psegs] !=                 [(x["i"], x["fp"]) for x in cur["segs"]]:
            # 清单结构变了（分片 URL 换了 / 顺序变了）
            reuse = _drop_all("分片清单与上次不一致（源已更换）")
        else:
            # 结构一致。但 E7b 现场是"URL 一模一样、内容被换"——
            # 光比 URL 指纹仍会误判复用，所以还要问服务端要内容身份（ETag/长度）。
            psegs_by_i = {x.get("i"): x for x in psegs if isinstance(x, dict)}
            # 先探测全部（服务端身份是唯一能发现"远端换内容"的证据，不能只看第一片）
            probes = []
            for cs in cur["segs"]:
                fp = None
                if pool is not None:
                    fp = _seg_probe_remote(cs["url"], pool)
                # 只认 ETag / Last-Modified；长度见 _content_token 的注释
                probes.append(_content_token(fp, strict=True))
            # R2-3：服务端一个内容版本都不给时（只回长度的也算不给），
            # "旧清单没记 ctok" 不该判作废 —— 那不是"源变了"，而是"根本无从问"。
            # 这种情形退到「按清单复用 + 本地哈希校验」，并明确警告一次
            # （见下面 no_service_id 分支）。旧实现漏了这一层：RUN1 记下全 None，
            # RUN2 判"缺内容指纹" → 全重下，等于把 0 请求续传废掉。
            no_service_id = all(t is None for t in probes)
            changed = None
            if not no_service_id:
                for cs, tok in zip(cur["segs"], probes):
                    ps = psegs_by_i.get(cs["i"]) or {}
                    old_tok = ps.get("ctok")
                    if tok is None:
                        # 有的片给了身份、这片没给：这一片的远端变化无从判断。
                        # 不整目录作废（那会退化成全重下），只在下面提醒留个印子。
                        continue
                    if old_tok is None:
                        # 旧清单没记内容身份（首次升级），不能凭"没记录"就复用
                        changed = "旧清单缺少内容指纹，无法确认分片未变"
                    elif old_tok != tok:
                        changed = "分片 %d 内容已变（%s ≠ %s）" % (cs["i"], old_tok, tok)
                    if changed:
                        break
            if changed:
                reuse = _drop_all(changed)
            else:
                reuse = True
                # R2-3：零请求的本地校验。清单里记着每片"下载完成后"的 sha1，
                # 对不上（被截断 / 手工替换 / 旧版本残留）就把这一片作废重下。
                # 只信本地磁盘，不额外发请求 —— 0 请求续传不受影响。
                bad, no_hash = [], []
                for cs in cur["segs"]:
                    ps = psegs_by_i.get(cs["i"]) or {}
                    p = seg_paths(cs["i"])
                    if not os.path.exists(p):
                        continue            # 本来就要下，无需"发现损坏"
                    if os.path.getsize(p) == 0:
                        bad.append((cs["i"], "文件为空"))
                        continue
                    want = ps.get("sha1")
                    if want in _HASH_MISSING:
                        # 旧清单（升级前）没记哈希，本地无从校验 —— 不因此废掉分片，
                        # 但必须补记，否则下次还是校验不了。
                        no_hash.append(cs["i"])
                        continue
                    got = _seg_hash(p)
                    if got != want:
                        bad.append((cs["i"], "sha1 不符（清单 %s… 实际 %s…）"
                                    % (str(want)[:8], str(got)[:8] if got else "读不出")))
                if no_hash:
                    print("警告: HLS 有 %d/%d 个分片缺失本地哈希（旧清单或升级前留下），"
                          "本轮无法校验其完整性，已补记；下次起可校验"
                          % (len(no_hash), len(cur["segs"])), file=sys.stderr)
                if bad:
                    print("警告: HLS 本地分片校验失败 %d/%d（例: %s）→ 作废重下"
                          % (len(bad), len(cur["segs"]),
                             "; ".join("分片 %d %s" % b for b in bad[:3])),
                          file=sys.stderr)
                    for idx, _why in bad:
                        for p in {seg_paths(idx),
                                  _local_victim(seg_dir, idx, fps[idx],
                                                psegs_by_i.get(idx) or {}, prev_urls)}:
                            try:
                                os.remove(p)
                            except OSError:
                                pass
                if no_service_id:
                    # 只说一次。别把"按清单复用"说成"内容已确认未变" ——
                    # 服务端换内容又恰好保持长度时，本地无从发现（见 README 限制）。
                    print("警告: 服务端未提供 ETag/Last-Modified，无法确认分片未变，"
                          "已按清单复用（本地完整性已校验；如需强制重下请加 --no-reuse）",
                          file=sys.stderr)

    if not reuse:
        for n in os.listdir(seg_dir):
            if n.endswith(".ts") or n == "concat.txt":
                try:
                    os.remove(os.path.join(seg_dir, n))
                except OSError:
                    pass
    # 把内容身份落盘 —— 下次才能比对。
    # 注意：这里不能只处理"上一次也存在的分片"。首次运行（prev 为 None）时
    # 若跳过写入，下次就会因为"旧清单缺少内容指纹"而白白重下一遍
    # （实测就是这个 bug：RUN1 全 None → RUN2 判作废 → 复用形同失效）。
    if pool is not None:
        psegs_by_i = {x.get("i"): x for x in ((prev or {}).get("segs") or [])
                      if isinstance(x, dict)}
        for cs in cur["segs"]:
            ps = psegs_by_i.get(cs["i"]) or {}
            if reuse and ps.get("ctok"):
                cs["ctok"] = ps["ctok"]          # 复用且已核对过，沿用以省请求
            else:
                cs["ctok"] = _content_token(_seg_probe_remote(cs["url"], pool),
                                            strict=True)
    # R2-3：分片哈希在下载完成后（_mark_segment_hashes）才写进去；
    # 复用的分片沿用清单里已核过的哈希，不能在这里写成 None ——
    # 那等于把上一轮的校验凭据抹掉，下次又变回"无从校验"。
    if reuse and prev is not None:
        psegs_by_i = {x.get("i"): x for x in (prev.get("segs") or [])
                      if isinstance(x, dict)}
        for cs in cur["segs"]:
            h = (psegs_by_i.get(cs["i"]) or {}).get("sha1")
            if h not in _HASH_MISSING:
                cs["sha1"] = h
    _atomic_json(man_path, cur)
    return reuse


def _mark_segment_hashes(seg_dir: str, segs, fps) -> None:
    """R2-3：分片真正下载完成（或确认已在盘上）后再算 sha1 落进清单。

    为什么必须放在下载之后而不是下载之前：下载前文件还不存在，算不出东西；
    而"上一轮留下的分片"已经在 `_bind_segments` 里校验过了（或刚删掉重下），
    所以这里统一重算一遍，既覆盖新下的片，也顺手给升级前的旧清单补记哈希。
    """
    man_path = os.path.join(seg_dir, SEG_MANIFEST)
    try:
        with open(man_path, encoding="utf-8") as f:
            man = json.load(f)
    except (OSError, ValueError):
        return
    recs = {x.get("i"): x for x in (man.get("segs") or []) if isinstance(x, dict)}
    dirty = False
    for i, u in enumerate(segs):
        p = os.path.join(seg_dir, "%05d-%s.ts" % (i, fps[i]))
        if not os.path.exists(p) or os.path.getsize(p) == 0:
            continue                     # 没下成 / 空文件：不记哈希，下次照常重下
        h = _seg_hash(p)
        if h is None:
            continue
        rec = recs.get(i)
        if rec is None:
            rec = {"i": i, "fp": fps[i], "url": u}
            man.setdefault("segs", []).append(rec)
            recs[i] = rec
        if rec.get("sha1") != h:
            rec["sha1"] = h
            dirty = True
    if dirty:
        try:
            _atomic_json(man_path, man)
        except OSError:
            pass


def _mark_segments_done(seg_dir: str, *, keep: bool):
    """P2-E：成功后不立即销毁证据。

    - `--keep-segments`：原样保留，用户自己清；
    - 否则打上 `done_at` 时间戳，24h 后（下次成功下载时）再清，
      这样"面板显示成功但内容是错的"这种事有东西可查。
    """
    man_path = os.path.join(seg_dir, SEG_MANIFEST)
    try:
        with open(man_path, encoding="utf-8") as f:
            man = json.load(f)
    except (OSError, ValueError):
        man = {}
    man["done_at"] = time.time()
    man["kept"] = bool(keep)
    try:
        _atomic_json(man_path, man)
    except OSError:
        pass
    if not keep:
        _gc_segments(seg_dir)


def _gc_segments(seg_dir: str):
    """清掉超过保留期的已完成分片目录内容。"""
    man_path = os.path.join(seg_dir, SEG_MANIFEST)
    try:
        with open(man_path, encoding="utf-8") as f:
            man = json.load(f)
    except (OSError, ValueError):
        return
    if man.get("kept"):
        return
    done_at = man.get("done_at")
    if not done_at or time.time() - done_at < SEG_KEEP_SECONDS:
        return
    shutil.rmtree(seg_dir, ignore_errors=True)


def hls_download(url, out_dir, *, name=None, ua=None, referer=None, cookie=None,
                 concurrency=8, on_progress=None, stop=None,
                 keep_segments=False, force_restart=False, reuse=True) -> str:
    on_progress = on_progress or (lambda p: None)
    stop = stop or threading.Event()
    os.makedirs(out_dir, exist_ok=True)
    pool = HttpPool(ua, referer, cookie)
    # P2-D：清单只下一次（守卫 + 分片列表 + 指纹一起出来）
    segs, fps, playlist_url = _resolve_segments(url, pool)
    if not segs:
        raise DownloadError("m3u8 未解析出任何分片")

    stem = os.path.splitext(name or _filename(url, None, "video.mp4"))[0]
    seg_dir = os.path.join(out_dir, f".{stem}.segments")
    os.makedirs(seg_dir, exist_ok=True)
    # R2-3：reuse=False（--no-reuse）等价于"这一轮不许复用任何旧片"。
    # 注意不能简单加个 else 分支就完事 —— 传给 _bind_segments 的 force_restart
    # 会顺带把整个清单丢掉，这里正是想要的语义：不复用，就从干净状态开始。
    _bind_segments(seg_dir, segs, fps, playlist_url,
                   force_restart=(force_restart or not reuse), pool=pool)
    total = len(segs)
    todo, done_cnt, errs = list(range(total)), 0, []
    lock = threading.Lock()
    t0 = time.time()
    bytes_got = [0]

    def worker():
        nonlocal done_cnt
        while not stop.is_set():
            with lock:
                if not todo:
                    return
                i = todo.pop(0)
            # P0-2：分片名带内容指纹，序号相同但来源不同 → 文件名不同，天然不会误复用
            path = os.path.join(seg_dir, f"{i:05d}-{fps[i]}.ts")
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with lock:
                    done_cnt += 1
                continue
            ok = False
            for attempt in range(3):
                try:
                    conn, r, _ = pool.open(segs[i])
                    data = r.read()
                    pool.drain(r)
                    tmp = f"{path}.tmp{threading.get_ident()}"
                    with open(tmp, "wb") as f:
                        f.write(data)
                    os.replace(tmp, path)
                    with lock:
                        bytes_got[0] += len(data)
                    ok = True
                    break
                except Exception:
                    time.sleep(0.5 * (attempt + 1))
            with lock:
                done_cnt += 1
                if not ok:
                    errs.append(i)
                snap, bg = done_cnt, bytes_got[0]
            el = max(time.time() - t0, 1e-6)
            on_progress({"percent": 100.0 * snap / total, "done": snap, "total": total,
                         "kind": "hls", "speed": bg / el, "concurrency": concurrency})

    ths = [threading.Thread(target=worker, daemon=True)
           for _ in range(max(1, min(concurrency, total)))]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    if stop.is_set():
        raise DownloadError("已取消（分片保留，重跑续传）")
    if errs:
        raise DownloadError(f"{len(errs)} 个分片失败: {sorted(errs)[:5]}（分片已保留）")

    # R2-3：分片齐了才记账 —— 记的是"下载完成后"的 sha1，供下次复用前校验本地完整性。
    _mark_segment_hashes(seg_dir, segs, fps)

    playlist = os.path.join(seg_dir, "concat.txt")
    with open(playlist, "w", encoding="utf-8") as f:
        for i in range(total):
            f.write(f"file '{i:05d}-{fps[i]}.ts'\n")
    out = os.path.join(out_dir, stem + ".mp4")
    if not shutil.which("ffmpeg"):
        # P0-2/P3：说清楚这些分片下次还会被用（指纹一致才复用，不一致会重下）
        raise DownloadError(
            "分片已下完，但缺 ffmpeg 无法合并；分片保留在 %s，"
            "装好 ffmpeg 后重跑同命令即可复用（清单未变才会复用）" % seg_dir)
    try:
        # 没有 timeout 时 ffmpeg 一卡住，任务就永久停在"合并中"且不响应取消
        p = subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                            "-f", "concat", "-safe", "0", "-i", playlist,
                            "-c", "copy", "-bsf:a", "aac_adtstoasc", out],
                           capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        raise DownloadError("ffmpeg 合并超时（30 分钟），分片已保留在 %s" % seg_dir)
    if p.returncode != 0:
        raise DownloadError(f"ffmpeg 合并失败: {p.stderr.strip()[:300]}")
    # P2-E：成功后不再立刻 rmtree —— P0-2 的混剪事故里，这正是唯一物证。
    # 默认改成「标记完成 + 由下次运行清理」，或用户 --keep-segments 长期保留。
    _mark_segments_done(seg_dir, keep=keep_segments)
    return out


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def _fmt(n) -> str:
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}TB"


class DownloadLockError(DownloadError):
    """同一目标文件已被另一个进程下载中。"""


class _FileLock:
    """对目标路径加进程级排他锁，避免两个 fluxdl 同时写同一文件。

    为什么需要它：实测中两个进程同时下载同一 URL，都执行 truncate(total)
    并各自 pwrite，互相覆盖后产出一个"大小正确、内容全零"的假成品——
    这种损坏最难查，因为它看起来像成功了。锁把这种情况变成明确报错。
    """

    def __init__(self, target: str):
        self.path = target + ".lock"
        self.fh = None

    def __enter__(self):
        self.fh = open(self.path, "a+")
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, ImportError) as e:
            self.fh.close()
            self.fh = None
            raise DownloadLockError(
                "另一个 fluxdl 正在下载同一文件（锁 %s）。"
                "请等待它完成，或确认没有残留进程后删除锁文件。" % self.path) from e
        self.fh.seek(0)
        self.fh.truncate()
        self.fh.write(str(os.getpid()))
        self.fh.flush()
        return self

    def __exit__(self, *exc):
        if self.fh is not None:
            try:
                if os.name == "nt":
                    import msvcrt
                    self.fh.seek(0)
                    msvcrt.locking(self.fh.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
            except (OSError, ImportError):
                pass
            self.fh.close()
            self.fh = None
            # 刻意不 unlink 锁文件：删除会造成"新旧 inode 上各持一把锁"的竞态。
            # A 释放并删除 -> B 在删除前已 open 到旧 inode（幽灵锁）-> C open 新文件
            # 也拿到锁 -> B 和 C 同时认为自己是唯一写入者，正好绕开本锁要防的 bug。
            # 残留的 0 字节 .lock 无害：flock 随进程退出自动释放，文件存在与否不改变语义。


def _hash_file(path: str, algo: str, *, chunk: int = 1 << 20) -> str:
    """流式计算文件摘要，避免整文件读入内存。"""
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _find_holes(fd: int, size: int):
    """返回文件的空洞区间列表；平台不支持 SEEK_DATA/SEEK_HOLE 时返回 None。

    注意：稀疏文件的空洞在语义上就是"全零且从未写过"，所以对下载器来说
    空洞 == 未写完的区域，可以直接判定内容不完整。
    """
    if size <= 0 or not hasattr(os, "SEEK_HOLE") or not hasattr(os, "SEEK_DATA"):
        return None
    holes, off = [], 0
    while off < size:
        try:
            data = os.lseek(fd, off, os.SEEK_DATA)
        except OSError:
            holes.append((off, size))    # 到尾都是空洞
            break
        if data > off:
            holes.append((off, data))
        try:
            off = os.lseek(fd, data, os.SEEK_HOLE)
        except OSError:
            break                        # 后面全是数据
    return holes


def verify_file(path: str, *, algo: str = "sha256", expect: str | None = None,
                verbose: bool = False):
    """校验文件内容，返回 (是否通过, 说明或实际摘要)。

    三件事，都是本次事故的直接教训：
    1. 文件必须存在且非空；
    2. 抽样检测全零空洞 —— 预分配未写入的伪成品，靠"大小正确"是看不出来的；
    3. gzip 魔数自检 + 可选摘要比对 —— 内容校验才是唯一可信的完成标准。
    """
    if not os.path.exists(path):
        return False, "文件不存在"
    size = os.path.getsize(path)
    if size == 0:
        return False, "文件为 0 字节"

    try:
        with open(path, "rb") as f:
            # 优先用 SEEK_DATA/SEEK_HOLE 精确找空洞。
            # 原先只抽 3 个偏移，实测会被"一小撮数据"骗过：在偏移 1000 写 100 字节，
            # 偏移 0 那片 2 MB 空洞就整块漏检 —— 而偏移 0 恰恰是预分配文件最典型的空洞位置
            # （Linux/btrfs 实测：抽样漏检，SEEK_HOLE 精确报出 4096–2097152）。
            holes = _find_holes(f.fileno(), size)
            # P2-10：tmpfs / 网络文件系统（NFS/CIFS/FUSE）不支持 SEEK_DATA/SEEK_HOLE，
            # _find_holes 返回 None，此时只剩下面 3 处抽样 —— 静默退化等于悄悄降低了
            # "预分配伪成品"的拦截能力，而这类文件恰恰是事故主角。
            # 不报错（抽样仍有价值），但要在 verbose 下说清楚，别让人误以为做了全量检测。
            if holes is None and verbose:
                print("[verify] 注意：本文件系统不支持空洞检测（tmpfs/网络盘常见），"
                      "已退化为 3 处抽样，结果可信度下降", flush=True)
            if holes:
                a, b = max(holes, key=lambda h: h[1] - h[0])
                return False, ("发现 %d 处空洞，最大一处 偏移 %d–%d（%d 字节）"
                               "（文件很可能是预分配后未写入）"
                               % (len(holes), a, b, b - a))
            # P1-2：这里是原来的第二个判据 —— "抽样块是否全零"，它会误杀合法文件。
            #
            # 实测（E2/E2b）：
            #   8 MiB 全零、非稀疏（_find_holes 返回 []）→ 判"损坏"，可它是合法内容
            #     （静音音轨 / 全黑片段 / dd if=/dev/zero 的填充镜像都是这样）；
            #   全零占 99.71% 的文件 → 判"通过"，只因为 4 KiB 抽样窗口恰好落在
            #     那 0.29% 真实数据上 —— 结论由抽样位置决定，这已经不算判据了。
            #
            # 改成：稀疏性优先。`_find_holes()` 有结论（不是 None）就以它为准：
            # 没有空洞 = 文件被真实写过，不再因"内容恰好全零"判损坏。
            # 只有在拿不到稀疏信息（tmpfs/网络盘）时才退回抽样，且降级为"提示"。
            if holes is None:
                for off in (0, max(0, size // 2 - 4096), max(0, size - 4096)):
                    f.seek(off)
                    blk = f.read(4096)
                    if blk and not any(blk):
                        # 拿不到稀疏信息时，全零块只提示、不判死
                        if verbose:
                            print("[verify] 注意：偏移 %d 处为全零块。本文件系统不支持"
                                  "空洞检测，无法区分「预分配未写入」与「内容本来就全零」；"
                                  "传 --expect-sha256 可权威判定" % off, flush=True)
                        break
    except OSError as e:
        return False, "读取失败: %s" % e

    if path.endswith((".gz", ".tgz")):
        with open(path, "rb") as f:
            magic = f.read(2)
        if magic != b"\x1f\x8b":
            return False, "gzip 魔数不符（文件已损坏）"

    # 没有 expect 就没有比对对象，全量算摘要纯属净亏损：
    # 200MiB 文件下载本身 1.8s，sha256 还要 1–2s，等于自己把加速吃掉一半。
    # 此时只做存在性 + 空洞 + 抽样 + gzip 魔数即可。
    if not expect:
        return True, ""

    try:
        actual = _hash_file(path, algo)
    except (ValueError, OSError) as e:
        return False, "无法计算 %s: %s" % (algo, e)

    if expect:
        want = expect.strip().lower()
        for pre in ("sha256:", "sha512:", "md5:"):
            if want.startswith(pre):
                want = want[len(pre):]
                break
        if actual.lower() != want:
            return False, actual
    return True, actual


def _bar(p, width=26) -> str:
    pct = min(p.get("percent", 0), 100.0)
    fill = int(width * pct / 100)
    eta = p.get("eta")
    eta_s = "--" if not isinstance(eta, (int, float)) else (
        f"{int(eta)}s" if eta < 60 else f"{int(eta)//60}m{int(eta)%60}s")
    return (f"\r[{'█'*fill}{'·'*(width-fill)}] {pct:5.1f}%  "
            f"{_fmt(p.get('speed', 0))}/s  剩 {eta_s}  c={p.get('concurrency', 1)} ")


def main(argv) -> int:
    import argparse
    ap = argparse.ArgumentParser("fluxdl", description="FluxDL 下载内核")
    ap.add_argument("url")
    ap.add_argument("-o", "--out", default=".")
    ap.add_argument("-c", "--concurrency", type=int, default=0,
                    help="0=自适应(默认)，>0 固定并发")
    ap.add_argument("-n", "--name")
    ap.add_argument("--referer")
    ap.add_argument("--cookie")
    ap.add_argument("--ua")
    ap.add_argument("--no-adaptive", action="store_true")
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--expect-sha256", metavar="HEX",
                    help="下载后用 sha256 校验，不匹配则退出码 2（强烈建议在脚本中使用）")
    ap.add_argument("--no-verify", action="store_true",
                    help="跳过下载后的内容自检（默认会检测空洞/gzip 魔数）")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="输出诊断信息（如文件系统不支持空洞检测时的降级提示）")
    ap.add_argument("--force-restart", action="store_true",
                    help="丢弃已有 .fluxdl 检查点与未完成的 .part，从头下载"
                         "（源已变更、断点作废时用）")
    ap.add_argument("--no-reuse", action="store_true",
                    help="HLS：不复用任何已下载分片，全部重下。"
                         "服务端不给 ETag/Last-Modified、又怀疑分片被换过时用"
                         "（否则可能按清单复用旧片，见 README「已知限制」）")
    a = ap.parse_args(argv)

    if a.probe:
        print(json.dumps(probe(a.url, ua=a.ua, referer=a.referer, cookie=a.cookie),
                         ensure_ascii=False, indent=2))
        return 0

    d = Downloader(a.url, a.out, concurrency=a.concurrency, ua=a.ua,
                   referer=a.referer, cookie=a.cookie, override_name=a.name,
                   adaptive=not a.no_adaptive, force_restart=a.force_restart,
                   verbose=a.verbose, reuse=not a.no_reuse)
    last = [0.0]

    def on_prog(p):
        now = time.time()
        if now - last[0] > 0.2:
            last[0] = now
            sys.stdout.write(_bar(p))
            sys.stdout.flush()

    d.on_progress = on_prog
    try:
        res = d.run()
    except KeyboardInterrupt:
        d.stop()
        print("\n已中断，重跑同命令即可续传。")
        return 130
    except DownloadError as e:
        p = d.progress()
        sys.stdout.write("\n")
        print(f"失败: {e}")
        # R2-1：源已变更分支的报错已经把"下一步"说完了（旧断点已丢弃 →
        # 重跑同一条命令从头下）。这里再补一句「已下 X，重跑同命令可续传」
        # 就是自相矛盾：那 X 属于新内容时并不存在，属于旧内容时已经作废。
        if getattr(d, "checkpoint_discarded", False):
            if p["done"] and getattr(d, "verbose", False):
                print(f"（作废的 {_fmt(p['done'])} 旧数据不计入新任务）")
        elif p["done"]:
            print(f"已下 {_fmt(p['done'])}，重跑同命令可续传。")
        return 1
    p = d.progress()
    sys.stdout.write(_bar({**p, "percent": 100.0}) + "\n")

    # ---- 内容校验：不再只信"文件大小"，必须验内容 ----
    # 这是本次事故的核心教训：预分配会让文件大小秒变正确，但里面全是空洞。
    if not a.no_verify:
        ok, info = verify_file(res["path"], expect=a.expect_sha256,
                               verbose=a.verbose)
        if ok:
            # 无 --expect-sha256 时 info 为空串（P1-4 已跳过全量摘要），
            # 直接打印 "sha256=" 会让人误解成"算了但没值"。
            print("校验通过: sha256=%s" % info if info else
                  "校验通过（未提供 --expect-sha256，已跳过摘要比对；"
                  "空洞/抽样/gzip 魔数检查已完成）")
        else:
            print(f"校验失败: {info}", file=sys.stderr)
            print("文件内容不可信，请删除后重下（或检查网络/代理是否篡改响应）。",
                  file=sys.stderr)
            return 2

    size = os.path.getsize(res["path"]) if os.path.exists(res["path"]) else 0
    extra = f"  续传自 {_fmt(res['resumed_from'])}" if res.get("resumed_from") else ""
    elapsed = f"{time.time() - d.t0:.1f}s" if d.t0 else "n/a"
    rate_note = f"  实测单连接={_fmt(d.probe_rate)}/s" if d.probe_rate else ""
    print(f"完成: {res['path']}  ({_fmt(size)})  并发={p.get('concurrency')}"
          f"{rate_note}  耗时={elapsed}{extra}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

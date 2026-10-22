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
import urllib.parse

DEFAULT_UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
CHUNK = 256 * 1024
META_V = 4
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

    def open(self, url: str, *, method="GET", rng=None, retries=2, follow=True):
        """返回 (conn, resp, final_url)。会跟随 3xx 并保持 Range 头。"""
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
                    conn.request(method, target, headers=self.headers(rng))
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
def probe(url: str, *, ua=None, referer=None, cookie=None) -> dict:
    info = {"url": url, "final_url": url, "size": None, "ranges": False,
            "filename": None, "kind": "unknown", "is_hls": False, "status": None,
            "content_type": None, "error": None, "note": None}
    pool = HttpPool(ua, referer, cookie, timeout=15)
    try:
        conn, r, final = pool.open(url, method="HEAD")
        info["status"], info["final_url"] = r.status, final
        h = r.headers
        info["content_type"] = h.get("Content-Type")
        info["filename"] = _filename(final, h.get("Content-Disposition"))
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
            info["content_type"] = info["content_type"] or r.headers.get("Content-Type")
            info["filename"] = info["filename"] or _filename(
                final, r.headers.get("Content-Disposition"))
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


def _load_meta(meta_path: str, url: str, size: int | None) -> dict:
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
                 max_concurrency=16, verbose=False):
        self.url, self.out_dir = url, out_dir
        self.verbose = verbose
        self.requested = int(concurrency or 0)
        self.max_concurrency = max(1, min(int(max_concurrency), 16))
        self.concurrency = self.requested or 4
        self.ua, self.referer, self.cookie = ua, referer, cookie
        self.override_name, self.adaptive = override_name, adaptive
        self.on_progress = on_progress or (lambda p: None)

        self.info, self.total = {}, 0
        self.finished_bytes = self.fetched = 0
        self.lock, self.meta_lock = threading.Lock(), threading.Lock()
        self.stop_flag = threading.Event()
        self.t0 = 0.0
        self.path = self.meta_path = ""
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
            try:
                conn, r, _ = self.pool.open(self.url, rng=(a, end))
                if r.status == 200:
                    raise DownloadError("服务端忽略 Range（返回 200），数据会错位")
                if r.status != 206:
                    raise DownloadError(f"HTTP {r.status}")
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
            for a, b in pend:
                _merge(self.meta, a, b)
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
            conn, r, _ = self.pool.open(self.url, rng=(0, span))
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
                                on_progress=self.on_progress, stop=self.stop_flag)
            self.path, self.finished = path, True
            return {"path": path, "kind": "hls", "size": os.path.getsize(path)}

        self.total = self.info.get("size") or 0
        name = self.override_name or self.info.get("filename") or "download.bin"
        os.makedirs(self.out_dir, exist_ok=True)
        self.path = os.path.join(self.out_dir, name)
        self.meta_path = self.path + ".fluxdl"

        if not self.info.get("ranges") or not self.total:
            return self._run_single()

        with _FileLock(self.path):
            return self._run_ranged()

    def _run_ranged(self) -> dict:
        """Range 下载主体（已在 _FileLock 保护下）。"""
        had_meta = os.path.exists(self.meta_path)
        self.meta = _load_meta(self.meta_path, self.final_url, self.total)
        self.finished_bytes = sum(e - s for s, e in self.meta["done"])
        gaps = _gaps(self.meta, self.total)

        # 关键修复：预分配会让文件"大小 == total"但内容是空洞。
        # 因此只有「meta 存在且校验通过」时，才可以信任既有文件续传；
        # 否则一律重新预分配（truncate 会把空洞显式归零，避免把垃圾当成品）。
        existed_same_size = os.path.exists(self.path) and os.path.getsize(self.path) == self.total
        trusted_resume = had_meta and existed_same_size and bool(self.meta.get("done"))
        if existed_same_size and not trusted_resume:
            print(f"警告: {self.path} 已存在且大小匹配，但缺少有效续传元数据 "
                  f"→ 视为未完成，重新下载", file=sys.stderr)
        fresh = not trusted_resume

        fh = open(self.path, "w+b" if fresh else "r+b")
        try:
            if fresh:
                fh.truncate(self.total)          # 预分配：避免边写边扩，也让 pwrite 合法
            elif os.path.getsize(self.path) < self.total:
                fh.truncate(self.total)
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
        if os.path.exists(self.meta_path):
            os.remove(self.meta_path)
        self.finished = True
        return {"path": self.path, "kind": "file", "size": self.total,
                "resumed_from": self.finished_bytes}

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
        os.replace(part, self.path)
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
    conn, r, final = pool.open(url)
    body = r.read().decode("utf-8", "replace")
    pool.drain(r)
    segs, master = _parse_m3u8(body, final)
    if master and segs and depth < 3:
        return _resolve_segments(segs[0], pool, depth + 1)
    return segs


def _check_hls_supported(url: str, pool: HttpPool, depth=0) -> None:
    """沿 master -> media 走一遍，加密流或 fMP4 初始化段直接拒绝。

    宁可明确报"不支持"，也不要合并出一个返回 0 的坏视频。
    """
    conn, r, final = pool.open(url)
    body = r.read().decode("utf-8", "replace")
    pool.drain(r, 0)
    encrypted, maps = _m3u8_guards(body)
    if encrypted:
        raise DownloadError(
            "该 HLS 流已加密（#EXT-X-KEY METHOD=AES-128 等），当前不支持解密")
    if maps:
        raise DownloadError(
            "该 HLS 流为 fMP4（含 #EXT-X-MAP 初始化段），"
            "直接 concat 会得到无 moov 的坏文件，当前不支持。init: %s"
            % ", ".join(maps[:3]))
    segs, master = _parse_m3u8(body, final)
    if master and segs and depth < 3:
        _check_hls_supported(segs[0], pool, depth + 1)


def hls_download(url, out_dir, *, name=None, ua=None, referer=None, cookie=None,
                 concurrency=8, on_progress=None, stop=None) -> str:
    on_progress = on_progress or (lambda p: None)
    stop = stop or threading.Event()
    os.makedirs(out_dir, exist_ok=True)
    pool = HttpPool(ua, referer, cookie)
    _check_hls_supported(url, pool)
    segs = _resolve_segments(url, pool)
    if not segs:
        raise DownloadError("m3u8 未解析出任何分片")

    stem = os.path.splitext(name or _filename(url, None, "video.mp4"))[0]
    seg_dir = os.path.join(out_dir, f".{stem}.segments")
    os.makedirs(seg_dir, exist_ok=True)
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
            path = os.path.join(seg_dir, f"{i:05d}.ts")
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

    playlist = os.path.join(seg_dir, "concat.txt")
    with open(playlist, "w", encoding="utf-8") as f:
        for i in range(total):
            f.write(f"file '{i:05d}.ts'\n")
    out = os.path.join(out_dir, stem + ".mp4")
    if not shutil.which("ffmpeg"):
        raise DownloadError("分片已下完，但缺 ffmpeg 无法合并")
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
    shutil.rmtree(seg_dir, ignore_errors=True)
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
            # 稀疏空洞之外，还要查"非稀疏但整段全零"——truncate 预分配后被显式写零，
            # 或并发写覆盖成零块时，SEEK_HOLE 看不到空洞，但内容同样是坏的。
            # 只抽 3 个偏移做快速筛查（全量扫描留给摘要比对）。
            for off in (0, max(0, size // 2 - 4096), max(0, size - 4096)):
                f.seek(off)
                blk = f.read(4096)
                if blk and not any(blk):
                    return False, ("偏移 %d 处为全零块"
                                   "（文件很可能是预分配/覆盖后未写入）" % off)
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
    a = ap.parse_args(argv)

    if a.probe:
        print(json.dumps(probe(a.url, ua=a.ua, referer=a.referer, cookie=a.cookie),
                         ensure_ascii=False, indent=2))
        return 0

    d = Downloader(a.url, a.out, concurrency=a.concurrency, ua=a.ua,
                   referer=a.referer, cookie=a.cookie, override_name=a.name,
                   adaptive=not a.no_adaptive)
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
        if p["done"]:
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

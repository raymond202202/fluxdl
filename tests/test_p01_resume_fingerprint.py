"""P0-1 回归：续传必须绑定内容（Hermes 复审 v4 第 2 节）。

核心断言：**源换了内容之后，绝不能按旧偏移拼出来一个"能打开"的文件。**

这三个用例对应复审报告 E1/E4 的实测复现路径：
  A. 源没变 → 续传照常成功，成品 == 源（证明绑定没把正常续传搞坏）；
  B. 源同大小不同内容 → 必须失败，且文案含「源已变更」，成品不出现；
  C. 同 B 但加 --force-restart → 必须成功，成品 == 新源。

运行：python3 -m unittest discover -s tests -v
"""
import contextlib
import hashlib
import http.server
import io
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fluxdl

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, "fluxdl.py")


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


class _EtagSource:
    """可换内容的 Range 源站：同大小、不同字节、ETag 随内容变。

    这就是复审 E4 的现场 —— nginx 覆盖同名文件后 ETag 从
    "size-mtime-hashA" 变成 "size-mtime-hashB"，大小一个字节都不差。
    """
    payload = b"\x00"
    log = []                     # 每次请求的记录：(method, range, if_range, status)

    def __init__(self):
        self.etag = '"%d-1-aaaa"' % len(self.payload)

    def set_payload(self, data: bytes, etag: str):
        self.payload, self.etag = data, etag


def _make_handler(src: _EtagSource):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            self._headers_only = True
            self._serve(head=True)

        def do_GET(self):
            self._serve(head=False)

        def _serve(self, head):
            rng = self.headers.get("Range")
            ir = self.headers.get("If-Range")
            total = len(src.payload)
            # If-Range 不匹配 → 按 RFC 7233 必须回 200 全量，不走 206。
            honor_range = True
            if ir and ir != src.etag:
                honor_range = False
            if rng and rng.startswith("bytes=") and honor_range:
                a, _, b = rng[6:].partition("-")
                start, end = int(a or 0), int(b or total - 1)
                end = min(end, total - 1)
                chunk, code = src.payload[start:end + 1], 206
            else:
                chunk, code = src.payload, 200
            src.log.append(("HEAD" if head else "GET", rng, ir, code))
            self.send_response(code)
            if code == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", src.etag)
            self.end_headers()
            if not head:
                # 限速：不限速的话 48 MiB 在回环上几十毫秒就下完，
                # 根本来不及在中途打断 —— 那就测不到"续传"了。
                for off in range(0, len(chunk), 65536):
                    self.wfile.write(chunk[off:off + 65536])
                    time.sleep(0.02)

        def log_message(self, *a):
            pass

    return H


class _Server:
    def __init__(self, handler):
        self.port = _free_port()
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), handler)

    def __enter__(self):
        self.th = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.th.start()
        time.sleep(0.05)
        return f"http://127.0.0.1:{self.port}"

    def __exit__(self, *a):
        self.httpd.shutdown()
        self.httpd.server_close()


# 两个版本：内容完全不同，大小一模一样（E4 现场就是「大小一个字节都不差」）
_BLK = 1024 * 1024
SOURCE_A = (bytes(range(256)) * (_BLK // 256)) * 48        # 48 MiB
SOURCE_B = (bytes(range(255, -1, -1)) * (_BLK // 256)) * 48
ETAG_A = '"50331648-1-ea11"'
ETAG_B = '"50331648-2-319eaf"'


class _ResumeBase(unittest.TestCase):
    def setUp(self):
        self.src = _EtagSource()
        self.src.set_payload(SOURCE_A, ETAG_A)
        self.src.log = []
        self.td = tempfile.mkdtemp(prefix="fluxdl_p01_")
        self.addCleanup(lambda: __import__("shutil").rmtree(self.td, ignore_errors=True))

    @contextlib.contextmanager
    def _interrupted_session(self, plant_b=True):
        """在**同一个源站（同一 URL）**上：先真中断，再按需换内容，然后交还 base。

        为什么必须同一个 server：`_load_meta()` 会校验 `meta["url"] == 当前 url`，
        换个端口就等于换了 URL → 检查点被整体作废 → 走 fresh 全量下载，
        于是"续传"分支根本没被执行过（本用例最初就是这么假绿的）。
        """
        with _Server(_make_handler(self.src)) as base:
            p = subprocess.Popen(
                [sys.executable, CLI, base + "/blob.bin", "-o", self.td, "-c", "1"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            meta = os.path.join(self.td, "blob.bin.fluxdl")
            deadline = time.time() + 10
            while time.time() < deadline:
                try:
                    import json
                    m = json.load(open(meta, encoding="utf-8"))
                    if m.get("done"):
                        break
                except Exception:
                    pass
                time.sleep(0.02)
            p.terminate()
            p.wait(timeout=10)
            self.assertEqual(p.returncode, -15, "应被 SIGTERM 打断")
            # 同一个 URL 上换内容（E4 现场：大小一模一样、字节全不同）
            if plant_b:
                self.src.set_payload(SOURCE_B, ETAG_B)
            self.src.log = []
            yield base


class TestResumeBoundToContent(_ResumeBase):
    def test_A_unchanged_source_resumes_ok(self):
        """源没变 → 续传成功、成品 sha == 源 sha，且续传请求带 If-Range。"""
        with self._interrupted_session(plant_b=False) as base:
            rc = fluxdl.main([base + "/blob.bin", "-o", self.td, "-c", "1"])
            out = os.path.join(self.td, "blob.bin")
            self.assertEqual(rc, 0, "源未变时续传必须成功")
            self.assertEqual(_sha(out), hashlib.sha256(SOURCE_A).hexdigest(),
                             "续传结果必须与源完全一致")
            # 口径：只有「续传阶段的数据 GET」才必须带 If-Range；
            # 之前的 HEAD 探测、以及 fresh 路径的抽样 Range 本就不带。
            resuming = [r for r in self.src.log if r[0] == "GET" and r[1]]
            self.assertTrue(resuming, "续传阶段应当发出过带 Range 的 GET，实际: %r"
                            % (self.src.log[:5],))
            self.assertTrue(all(r[2] == ETAG_A for r in resuming),
                            "续传请求必须带 If-Range: <旧 ETag>，实际: %r" % (resuming[:3],))

    def test_B_changed_source_fails_loudly(self):
        """同大小不同内容 → 非 0 退出、文案含「源已变更」、残件不得通过校验。"""
        # 口径：fluxdl.main() 面向终端用户，失败文案打在 stdout（见 fluxdl.py
        # 的 `print(f"失败: {e}")`）；stderr 只放进度/警告等旁路信息。
        # 所以这里必须抓 stdout —— 抓 stderr 会永远拿到空串，然后误判"文案缺失"。
        err = io.StringIO()
        with self._interrupted_session(plant_b=True) as base, \
                contextlib.redirect_stdout(err):
            rc = fluxdl.main([base + "/blob.bin", "-o", self.td, "-c", "1"])
        out = os.path.join(self.td, "blob.bin")
        self.assertNotEqual(rc, 0, "源已变更必须非 0 退出")
        # 注意：blob.bin 在预分配阶段就存在（大小 == total 但含空洞），
        # 所以「成品不存在」是错的断言。真正判据是：残件不得通过校验。
        if os.path.exists(out):
            ok, why = fluxdl.verify_file(out)
            self.assertFalse(ok, "源已变更时磁盘文件不得通过校验（否则就是新旧混合）；"
                                 "verify_file 却返回通过: %r" % (why,))
        # 文案三要素：判定 + 新旧指纹 + 下一步
        msg = err.getvalue()
        self.assertIn("源已变更", msg)
        self.assertIn(ETAG_A, msg, "报错要给出旧指纹")
        self.assertIn(ETAG_B, msg, "报错要给出新指纹")
        self.assertIn("--force-restart", msg, "报错要给出下一步")

    def test_C_force_restart_recovers(self):
        """加 --force-restart → 退出 0，成品 == 新源。"""
        with self._interrupted_session(plant_b=True) as base:
            rc = fluxdl.main([base + "/blob.bin", "-o", self.td, "-c", "1",
                              "--force-restart"])
        out = os.path.join(self.td, "blob.bin")
        self.assertEqual(rc, 0, "--force-restart 应当成功，实际 rc=%r" % (rc,))
        self.assertEqual(_sha(out), hashlib.sha256(SOURCE_B).hexdigest(),
                         "force-restart 后成品必须是新源，不能是混合")
        self.assertFalse(os.path.exists(out + ".fluxdl"))


if __name__ == "__main__":
    unittest.main(verbosity=2)

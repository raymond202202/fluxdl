"""FluxDL 回归测试：Hermes 复审 v2 报出的两个 P0 + P1-4/P1-5。

这些都是"静默成功"类 bug —— 报错为零，但产物是坏的。
每个用例对应一次真实事故，故意写成断言而不是手工脚本：
文档会过期，断言不会。

运行：python3 -m unittest discover -s tests -v
"""
import hashlib
import http.server
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fluxdl


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _TruncatingHandler(http.server.BaseHTTPRequestHandler):
    """声明 Content-Length: 100000，实际只发 60000 字节就优雅关闭。

    这是 P0-1 的复现条件：真实世界里由 CDN 边缘、对象存储签名 URL、
    反向代理超时、服务端限速掐连接造成。
    """
    payload = b"\x01" * 60000
    declared = 100000

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(self.declared))
        # 关键：不返回 Accept-Ranges，逼内核走 _run_single() 单连接路径
        self.end_headers()
        self.wfile.write(self.payload)
        self.wfile.flush()

    def log_message(self, *a):
        pass


class _RangeServer(http.server.BaseHTTPRequestHandler):
    """正常支持 Range 的服务器，作为对照组。"""
    payload = bytes(range(256)) * 400   # 102400 字节，非零、非全同

    def do_GET(self):
        rng = self.headers.get("Range")
        total = len(self.payload)
        if rng and rng.startswith("bytes="):
            a, _, b = rng[6:].partition("-")
            start, end = int(a or 0), int(b or total - 1)
            end = min(end, total - 1)
            chunk = self.payload[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
        else:
            chunk = self.payload
            self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(chunk)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(chunk)

    def log_message(self, *a):
        pass


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


class TestTruncation(unittest.TestCase):
    """P0-1：截断文件不得被当成成品。"""

    def test_truncated_single_conn_raises(self):
        with _Server(_TruncatingHandler) as base, tempfile.TemporaryDirectory() as td:
            out = os.path.join(td, "half.bin")
            dl = fluxdl.Downloader(base + "/half.bin", td, concurrency=1)
            with self.assertRaises(fluxdl.DownloadError) as ctx:
                dl.run()
            msg = str(ctx.exception)
            self.assertIn("截断", msg)
            self.assertIn("100000", msg)
            self.assertIn("60000", msg)
            self.assertFalse(os.path.exists(out),
                             "截断的文件不应该出现在最终路径")
            self.assertFalse(os.path.exists(out + ".part"),
                             ".part 残件应被清理")

    def test_range_download_still_works(self):
        """对照组：正常 Range 服务器不受影响，且内容完整。"""
        with _Server(_RangeServer) as base, tempfile.TemporaryDirectory() as td:
            dl = fluxdl.Downloader(base + "/ok.bin", td, concurrency=4)
            res = dl.run()
            got = open(res["path"], "rb").read()
            self.assertEqual(got, _RangeServer.payload)


class TestVerifyNoExpect(unittest.TestCase):
    """P1-4：没有 expect 时不该全量算 sha256（净亏损）。"""

    def test_no_expect_skips_hash(self):
        with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
            f.write(b"\x02" * 8192)
            path = f.name
        try:
            called = []
            orig = fluxdl._hash_file

            def spy(*a, **kw):
                called.append(a)
                return orig(*a, **kw)

            fluxdl._hash_file = spy
            try:
                ok, info = fluxdl.verify_file(path)
            finally:
                fluxdl._hash_file = orig
            self.assertTrue(ok)
            self.assertEqual(called, [], "无 expect 时不应调用 _hash_file")
        finally:
            os.unlink(path)

    def test_with_expect_still_hashes(self):
        data = b"\x03" * 4096
        with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as f:
            f.write(data)
            path = f.name
        try:
            digest = hashlib.sha256(data).hexdigest()
            ok, info = fluxdl.verify_file(path, expect=digest)
            self.assertTrue(ok)
            self.assertEqual(info, digest)
            ok2, _ = fluxdl.verify_file(path, expect="0" * 64)
            self.assertFalse(ok2)
        finally:
            os.unlink(path)


class TestHlsGuards(unittest.TestCase):
    """P1-5：加密流 / fMP4 必须明确报错，不能拼出"成功"的坏视频。"""

    def test_aes_key_detected(self):
        enc, maps = fluxdl._m3u8_guards(
            '#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="k.key"\nseg0.ts')
        self.assertTrue(enc)
        self.assertEqual(maps, [])

    def test_method_none_is_fine(self):
        enc, maps = fluxdl._m3u8_guards('#EXTM3U\n#EXT-X-KEY:METHOD=NONE\nseg0.ts')
        self.assertFalse(enc)

    def test_ext_x_map_detected(self):
        enc, maps = fluxdl._m3u8_guards(
            '#EXTM3U\n#EXT-X-MAP:URI="init.mp4"\nseg0.m4s')
        self.assertEqual(maps, ["init.mp4"])


class TestTargetKey(unittest.TestCase):
    """P0-2：同名不同 URL 不能被判为同一目标。"""

    def setUp(self):
        import fluxdl_server
        self.srv = fluxdl_server

    def test_same_name_different_host_not_same(self):
        k1 = self.srv._target_key("https://a.com/v1/pack.zip", {})
        k2 = self.srv._target_key("https://b.com/v2/pack.zip", {})
        self.assertNotEqual(k1, k2, "不同 host/path 的同名文件不能判为同一目标")

    def test_same_name_different_path_not_same(self):
        k1 = self.srv._target_key(
            "https://github.com/x/proj/releases/download/v1/app.zip", {})
        k2 = self.srv._target_key(
            "https://github.com/x/proj/releases/download/v2/app.zip", {})
        self.assertNotEqual(k1, k2)

    def test_query_variants_are_same(self):
        """?u=1 / ?u=2 指向同一路径 —— 这仍是同一目标（保留原始诉求）。"""
        k1 = self.srv._target_key("https://a.com/f.zip?u=1", {})
        k2 = self.srv._target_key("https://a.com/f.zip?u=2", {})
        self.assertEqual(k1, k2)


if __name__ == "__main__":
    unittest.main(verbosity=2)

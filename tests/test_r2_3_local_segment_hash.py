"""R2-3 回归：服务端不给内容身份时，HLS 不能静默复用被本地改坏的旧片。

复现的现场（实测打出来的字）：
    RUN2 .ts GET 次数=0 成品==新源成品: False 成品==旧源成品: True rc=0

服务端返回 200 + 正确长度，但**没有 ETag / Last-Modified**（简易静态服务、
部分 CDN 就这样）。分片 URL 与长度都不变，重跑时：
  - 有指纹时能发现远端内容变了；
  - 没指纹时物理上做不到，本用例不苛求；
  - 但**本地文件坏了**（截断 / 手工替换 / 旧版本残留）必须能发现 ——
    那是纯本地信息，读盘就知道，一次网络请求都不该多花。

本文件锁三件事：
  1. 手工截断分片 → 检测到 + 重下该片 + 成品 sha256 与源一致（不能 rc=0 且成品坏）；
  2. 无指纹但内容没变 → 0 个 .ts 请求复用，且"无法确认分片未变"只警告**一次**；
  3. 有指纹且内容变了 → 仍然重下（R1 的判定不能回归）。

为什么单独一个文件而不是塞进 test_p02：那边用"画面颜色"当版本标记、服务端带
ETag；这边要的是一个**只回 Content-Length** 的服务端，两边服务端语义不同，
混在一个类里会互相干扰（尤其"是否算有身份"这条）。
"""
import contextlib
import hashlib
import http.server
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fluxdl

N_SEG = 4
_TSCACHE = {}


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _ts(color: str) -> bytes:
    """生成 1 秒纯色 TS（模块级缓存）；缺 libx264 时返回 b"" → 用例 skip。"""
    if color in _TSCACHE:
        return _TSCACHE[color]
    tmpd = tempfile.mkdtemp(prefix="fluxdl_r23_tpl_")
    try:
        out = os.path.join(tmpd, "a.ts")
        p = subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
             "-i", "color=c=%s:s=64x64:d=1:r=10" % color, "-c:v", "libx264",
             "-pix_fmt", "yuv420p", "-f", "mpegts", "-y", out],
            capture_output=True, timeout=60)
        if p.returncode == 0:
            with open(out, "rb") as f:
                _TSCACHE[color] = f.read()
        else:
            _TSCACHE[color] = b""
    except Exception:
        _TSCACHE[color] = b""
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)
    return _TSCACHE[color]


def _playlist(n=N_SEG) -> bytes:
    lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:2",
             "#EXT-X-MEDIA-SEQUENCE:0"]
    for i in range(n):
        lines += ["#EXTINF:2.0,", "seg%d.ts" % i]
    lines.append("#EXT-X-ENDLIST")
    return ("\n".join(lines) + "\n").encode()


class _Src:
    """可控源站。`identity=False` 时只回 Content-Length（R2-3 的现场）。"""

    def __init__(self):
        self.log = []
        self.identity = False
        self.color = "red"
        self.etag = "v1"
        self.playlist_hits = 0

    def body(self, path):
        return _ts(self.color)

    def ts_gets(self):
        return [p for m, p in self.log if m == "GET" and p.endswith(".ts")]

    def reset(self):
        self.log = []
        self.playlist_hits = 0


def _make_handler(src: _Src):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            self._serve(True)

        def do_GET(self):
            self._serve(False)

        def _serve(self, head_only):
            src.log.append(("HEAD" if head_only else "GET", self.path))
            if self.path.endswith(".m3u8"):
                src.playlist_hits += 1
                body, ctype = _playlist(), "application/vnd.apple.mpegurl"
            elif self.path.endswith(".ts"):
                body, ctype = src.body(self.path), "video/mp2t"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            if src.identity:
                self.send_header("ETag", '"%s"' % src.etag)
            self.end_headers()
            if not head_only:
                self.wfile.write(body)

        def log_message(self, *a):
            pass

    return H


class _Server:
    def __init__(self, handler):
        self.port = _free_port()
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), handler)
        self.th = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.th.start()
        time.sleep(0.05)
        return "http://127.0.0.1:%d" % self.port

    def __exit__(self, *a):
        self.httpd.shutdown()
        self.httpd.server_close()


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


@unittest.skipUnless(shutil.which("ffmpeg"), "需要 ffmpeg 才能合并分片")
class TestNoServerIdentity(unittest.TestCase):
    def setUp(self):
        if not _ts("red") or not _ts("blue"):
            self.skipTest("无法生成纯色 TS 模板（缺 libx264？）")
        self.src = _Src()
        self.td = tempfile.mkdtemp(prefix="fluxdl_r23_")
        self.addCleanup(lambda: shutil.rmtree(self.td, ignore_errors=True))
        self.seg_dir = os.path.join(self.td, ".clip.segments")

    def _run(self, base, **kw):
        """返回 (成品路径, stderr 文本)。"""
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            out = fluxdl.hls_download(base + "/v.m3u8", self.td, name="clip.mp4",
                                      concurrency=2, **kw)
        return out, buf.getvalue()

    def _manifest(self):
        with open(os.path.join(self.seg_dir, fluxdl.SEG_MANIFEST), encoding="utf-8") as f:
            return json.load(f)

    # ---------------------------------------------------------------- 验收 1
    def test_A_truncated_local_segment_is_detected(self):
        """手工把一片截断一半 → 必须发现、重下该片、成品 sha256 与源一致。"""
        with _Server(_make_handler(self.src)) as base:
            good, _ = self._run(base)
            src_sha = _sha256(good)
            man = self._manifest()
            self.assertEqual(sum(1 for s in man["segs"] if s.get("sha1")), N_SEG,
                             "R2-3：清单应记录每片下载完成后的 sha1")

            victim = sorted(n for n in os.listdir(self.seg_dir)
                            if n.startswith("00002-") and n.endswith(".ts"))[0]
            vp = os.path.join(self.seg_dir, victim)
            full = os.path.getsize(vp)
            with open(vp, "r+b") as f:
                f.truncate(full // 2)

            self.src.reset()
            out, err = self._run(base)
            ts_get = self.src.ts_gets()
            self.assertIn("本地分片校验失败", err, "发现本地分片损坏必须明确报出来")
            self.assertEqual(len(ts_get), 1,
                             "只该重下坏掉的那一片，实际 %d 片: %r"
                             % (len(ts_get), ts_get[:5]))
            self.assertEqual(os.path.getsize(vp), full, "被截断的分片应被重下到原大小")
            self.assertEqual(_sha256(out), src_sha,
                             "成品必须与源一致；rc=0 但成品是坏内容是不能接受的")

    def test_A2_replaced_local_segment_is_detected(self):
        """把一片换成别的内容（长度可以一样）→ 哈希对不上，同样要重下。"""
        with _Server(_make_handler(self.src)) as base:
            good, _ = self._run(base)
            src_sha = _sha256(good)
            other = _ts("blue")
            victim = sorted(n for n in os.listdir(self.seg_dir)
                            if n.startswith("00001-") and n.endswith(".ts"))[0]
            with open(os.path.join(self.seg_dir, victim), "wb") as f:
                f.write(other[:len(other) // 2])

            self.src.reset()
            out, err = self._run(base)
            self.assertIn("本地分片校验失败", err)
            self.assertEqual(len(self.src.ts_gets()), 1)
            self.assertEqual(_sha256(out), src_sha)

    # ---------------------------------------------------------------- 验收 2
    def test_B_no_identity_reuse_is_zero_request_and_warns_once(self):
        """服务端只给 Content-Length、内容没变 → 0 个 .ts 请求，警告恰好一次。"""
        with _Server(_make_handler(self.src)) as base:
            self._run(base)                      # RUN1：4 片全下
            self.src.reset()
            out, err = self._run(base)           # RUN2：应全复用

            self.assertEqual(self.src.ts_gets(), [],
                             "内容未变且服务端无身份时不该有任何 .ts 请求，实际 %r"
                             % (self.src.ts_gets()[:5],))
            # 只数 fluxdl 自己的警告行 —— stderr 里还混着 Python 的
            # ResourceWarning（未关闭的 socket），那是本用例噪声，不是产品行为。
            warns = [l for l in err.splitlines() if l.startswith("警告:")]
            self.assertEqual(len(warns), 1,
                             "应只警告一次（不是每片一次），实际 %d 行: %r"
                             % (len(warns), warns))
            self.assertIn("ETag", warns[0])
            self.assertIn("Last-Modified", warns[0])
            self.assertIn("无法确认分片未变", warns[0])
            self.assertIn("已按清单复用", warns[0])
            self.assertIn("--no-reuse", warns[0])

            # 再跑一轮：复用不会把警告攒成两遍，也不该重新开始下分片
            self.src.reset()
            self._run(base)
            self.assertEqual(self.src.ts_gets(), [])

    def test_B2_length_only_is_not_identity(self):
        """只回长度**不算**"有身份" —— 这是本轮的实测教训，锁死它。"""
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            man = self._manifest()
            for s in man["segs"]:
                # ctok 不能是 'l:6768' 这种"看起来有身份"的假身份
                self.assertFalse((s.get("ctok") or "").startswith("l:"),
                                 "纯长度不能当内容身份: %r" % (s.get("ctok"),))
            self.src.reset()
            _, err = self._run(base)
            self.assertIn("无法确认分片未变", err)

    # ---------------------------------------------------------------- 验收 3
    def test_C_identity_present_and_content_changed_still_redownloads(self):
        """有 ETag + 内容变了 → 沿用 R1 判定，必须重下（不能回归）。"""
        self.src.identity = True
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            # 同长度、同 URL、换内容，并且 ETag 跟着变（服务端诚实的表现）
            self.src.color = "blue"
            self.src.etag = "v2"
            self.src.reset()
            out, err = self._run(base)
            self.assertEqual(len(self.src.ts_gets()), N_SEG,
                             "指纹变了必须重下全部分片，实际 %d/%d"
                             % (len(self.src.ts_gets()), N_SEG))
            self.assertIn("内容已变", err)
            # 换源后成品不能还是旧内容
            self.assertNotEqual(_sha256(out), _sha256_of_color_ts("red"))

    def test_C2_identity_present_and_unchanged_reuses_silently(self):
        """有 ETag 且没变 → 0 请求，且**不该**出现"无法确认"警告。"""
        self.src.identity = True
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            self.src.reset()
            _, err = self._run(base)
            self.assertEqual(self.src.ts_gets(), [])
            self.assertNotIn("无法确认分片未变", err)

    # ---------------------------------------------------------------- 显式重下
    def test_D_no_reuse_switch_forces_full_redownload(self):
        """reuse=False（CLI --no-reuse）→ 全部重下，不受清单复用影响。"""
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            self.src.reset()
            out, _ = self._run(base, reuse=False)
            self.assertEqual(len(self.src.ts_gets()), N_SEG,
                             "reuse=False 应当重下全部分片，实际 %d"
                             % len(self.src.ts_gets()))
            self.assertTrue(os.path.exists(out))

    def test_E_cli_has_no_reuse_flag(self):
        """CLI 必须真的有 --no-reuse（文档里承诺的开关不能是空话）。"""
        import argparse
        with open(fluxdl.__file__, encoding="utf-8") as f:
            src = f.read()
        self.assertIn("--no-reuse", src)
        self.assertIn("reuse=not a.no_reuse", src)
        # 顺带确认 hls_download / Downloader 都接受 reuse 参数
        self.assertIn("reuse=True", src)
        self.assertIsInstance(argparse, object)

    # ---------------------------------------------------------------- 旧清单
    def test_F_old_manifest_without_hash_is_tolerated_and_backfilled(self):
        """升级前的旧清单没有 sha1 → 不能因此废掉分片，但要补记，下次可校验。"""
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            man_path = os.path.join(self.seg_dir, fluxdl.SEG_MANIFEST)
            man = self._manifest()
            for s in man["segs"]:
                s.pop("sha1", None)
            with open(man_path, "w", encoding="utf-8") as f:
                json.dump(man, f)

            self.src.reset()
            out, err = self._run(base)
            self.assertEqual(self.src.ts_gets(), [],
                             "旧清单没记哈希不该导致全量重下（那是 0 请求续传的退路）")
            self.assertIn("缺失本地哈希", err)
            again = self._manifest()
            self.assertEqual(sum(1 for s in again["segs"] if s.get("sha1")), N_SEG,
                             "本轮应当补记哈希，下次才能校验")


def _sha256_of_color_ts(color: str) -> str:
    tmpd = tempfile.mkdtemp(prefix="fluxdl_r23_chk_")
    try:
        p = os.path.join(tmpd, "x.ts")
        with open(p, "wb") as f:
            f.write(_ts(color))
        return _sha256(p)
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)

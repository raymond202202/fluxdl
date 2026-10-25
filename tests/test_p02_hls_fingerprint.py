"""P0-2 / P2-D / P2-E 回归：HLS 分片必须绑定内容，清单只能拉一次。

复现的是 Hermes 复审 v4 的 E5 / E7b —— 那条最要命的路径：
  1. 首轮失败（缺 ffmpeg / 中途被杀），分片留在 `.xxx.segments/`；
  2. 源站把分片换成**同大小不同内容**；
  3. 重跑同一条命令 —— 旧实现 0 个 .ts 请求，直接 concat，`rc=0` 报「完成」，
     成品既不是新源也不是旧源，而分片目录当场被 rmtree，证据自毁。

判据怎么来的（踩过的坑，记下来免得下次重犯）：
  - 想在 TS 尾部 null packet 里写版本标记 → `ffmpeg -c copy` 会把 null packet
    整个丢掉（实测 out 里两个版本标记都找不到），所以这条路不可行。
  - 最后用**画面颜色**当版本标记：编码后的像素在重封装后依然存在，
    可以用 ffmpeg 抽一帧读像素精确判断成品里是哪一版内容。
  - 分片大小必须完全一致（E7b 现场就是同大小），这里靠固定分辨率/帧率/时长保证。
"""
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
import http.server

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fluxdl

N_SEG = 4
COLOR_A, COLOR_B = "red", "blue"     # 版本由画面颜色区分（唯一能穿透 -c copy 的标记）
_TSCACHE = {}


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _ts_for_color(color: str) -> bytes:
    """生成 1 秒纯色 TS（模块级缓存）；失败返回 b'' → 用例 skip。"""
    if color in _TSCACHE:
        return _TSCACHE[color]
    tmpd = tempfile.mkdtemp(prefix="fluxdl_p02_tpl_")
    try:
        out = os.path.join(tmpd, "a.ts")
        p = subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi",
             "-i", "color=c=%s:s=64x64:d=1:r=10" % color, "-c:v", "libx264",
             "-pix_fmt", "yuv420p", "-f", "mpegts", "-y", out],
            capture_output=True, timeout=60)
        if p.returncode != 0 or not os.path.exists(out):
            _TSCACHE[color] = b""
        else:
            with open(out, "rb") as f:
                _TSCACHE[color] = f.read()
    except Exception:
        _TSCACHE[color] = b""
    finally:
        shutil.rmtree(tmpd, ignore_errors=True)
    return _TSCACHE.get(color, b"")


def _playlist(n=N_SEG):
    lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:2",
             "#EXT-X-MEDIA-SEQUENCE:0"]
    for i in range(n):
        lines += ["#EXTINF:2.0,", "seg%d.ts" % i]
    lines.append("#EXT-X-ENDLIST")
    return ("\n".join(lines) + "\n").encode()


class _HlsSource:
    """可控源站：分片整体换成「同数量、同大小、不同颜色」= E7b 现场。"""

    def __init__(self):
        self.log = []
        self.color = COLOR_A
        self.playlist_hits = 0

    def seg(self, i) -> bytes:
        return _ts_for_color(self.color)


def _make_handler(src: _HlsSource):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            # P0-2 的内容身份就靠 HEAD 拿（ETag/Last-Modified/Content-Length）。
            # 不实现的话会退化成 1 字节 Range 探测 —— 那也算一次 .ts 请求，
            # 会把"复用是否生效"的计数搅乱。
            self._serve(body_only=True)

        def do_GET(self):
            self._serve(body_only=False)

        def _serve(self, body_only):
            src.log.append(("HEAD" if body_only else "GET", self.path))
            if self.path.endswith(".m3u8"):
                src.playlist_hits += 1
                body, ctype = _playlist(), "application/vnd.apple.mpegurl"
            elif self.path.endswith(".ts"):
                try:
                    b = os.path.basename(self.path)
                    int(b[3:].split("-")[0].split(".")[0])
                except ValueError:
                    self.send_error(404)
                    return
                body, ctype = src.seg(0), "video/mp2t"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            if self.path.endswith(".ts"):
                # 同长度、不同内容的 ETag —— 正是 E7b 那种"看起来没变"的换源
                self.send_header("ETag", '"%s"' % src.color)
            self.end_headers()
            if not body_only:
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


def _center_pixel(path):
    """抽第 1 帧中心像素 → 判断成品是红源还是蓝源。"""
    r = subprocess.run(
        ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", path, "-frames:v", "1",
         "-f", "rawvideo", "-pix_fmt", "rgb24", "-"],
        capture_output=True, timeout=60)
    d = r.stdout
    if len(d) < 3 * (64 * 32 + 32) + 3:
        return None
    off = 3 * (64 * 32 + 32)
    return tuple(d[off:off + 3])


def _which_color(px):
    if px is None:
        return None
    r, g, b = px
    if r > 150 and g < 80 and b < 80:
        return COLOR_A           # red
    if b > 150 and r < 80 and g < 80:
        return COLOR_B           # blue
    return None


@unittest.skipUnless(shutil.which("ffmpeg") and shutil.which("ffprobe"),
                     "需要 ffmpeg + ffprobe")
class TestHlsFingerprint(unittest.TestCase):
    def setUp(self):
        if not _ts_for_color(COLOR_A) or not _ts_for_color(COLOR_B):
            self.skipTest("无法生成纯色 TS 模板（缺 libx264？）")
        self.src = _HlsSource()
        self.td = tempfile.mkdtemp(prefix="fluxdl_p02_")
        self.addCleanup(lambda: shutil.rmtree(self.td, ignore_errors=True))

    def _run(self, base, **kw):
        return fluxdl.hls_download(base + "/v.m3u8", self.td, name="clip.mp4",
                                   concurrency=2, **kw)

    def test_A_same_source_reuses_segments(self):
        with _Server(_make_handler(self.src)) as base:
            out = self._run(base)
            self.assertEqual(_which_color(_center_pixel(out)), COLOR_A,
                             "首轮成品必须是源的颜色")
            # 二跑：清单没变 → 分片应当全复用
            self.src.log, self.src.playlist_hits = [], 0
            out = self._run(base)
            ts_hits = [p for m_, p in self.src.log if m_ == "GET" and p.endswith(".ts")]
            self.assertEqual(ts_hits, [],
                             "清单未变时分片应当全部复用，实际又下了: %r" % (ts_hits[:3],))

    def test_B_changed_source_must_redownload(self):
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            # 换成同数量、同大小、不同颜色 —— E7b 现场
            self.src.color = COLOR_B
            self.src.log, self.src.playlist_hits = [], 0
            out = self._run(base)
            ts_hits = [p for m_, p in self.src.log if m_ == "GET" and p.endswith(".ts")]
            self.assertEqual(len(ts_hits), N_SEG,
                             "源已更换时必须重下全部分片，实际只下了 %d/%d: %r"
                             % (len(ts_hits), N_SEG, ts_hits[:5]))
            self.assertEqual(_which_color(_center_pixel(out)), COLOR_B,
                             "换源后成品必须是新源，不能是新旧混剪")

    def test_C_playlist_fetched_once(self):
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            self.assertEqual(self.src.playlist_hits, 1,
                             "P2-D：同一个 m3u8 只应 GET 一次，实际 %d 次"
                             % self.src.playlist_hits)

    def test_D_segments_survive_success(self):
        with _Server(_make_handler(self.src)) as base:
            self._run(base)
            seg_dir = os.path.join(self.td, ".clip.segments")
            self.assertTrue(os.path.isdir(seg_dir), "P2-E：成功后分片目录不应立刻消失")
            with open(os.path.join(seg_dir, fluxdl.SEG_MANIFEST), encoding="utf-8") as f:
                man = json.load(f)
            self.assertIn("done_at", man, "应打完成时间戳（供 24h 后回收）")

    def test_E_keep_segments_flag(self):
        with _Server(_make_handler(self.src)) as base:
            self._run(base, keep_segments=True)
            seg_dir = os.path.join(self.td, ".clip.segments")
            with open(os.path.join(seg_dir, fluxdl.SEG_MANIFEST), encoding="utf-8") as f:
                man = json.load(f)
            self.assertTrue(man.get("kept"), "--keep-segments 应当标记为长期保留")


if __name__ == "__main__":
    unittest.main(verbosity=2)

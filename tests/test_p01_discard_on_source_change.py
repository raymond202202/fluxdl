"""R2-1 回归：报「源已变更」时必须真的把检查点作废（任务书 R2-1）。

缺陷现场（实测）：
    4 MiB 源以 300KB/s 下 3.5s，SIGTERM 留下 768KB 断点；服务端把同一 URL
    的内容换成另一份（ETag "v-A" → "v-B"）后跑普通命令 —— 报
    「源已变更…断点已作废；加 --force-restart 可丢弃检查点重下」，紧接着
    又打印「已下 768.0KB，重跑同命令可续传」，而磁盘上的 meta 仍是
    {'done': [[0, 786432]], 'etag': '"v-A"'}。
    于是重跑普通命令永远是同一句报错，用户只能靠 --force-restart 自救。

本文件锁死三件事（对应用户要求的三步实测）：
  A. 造断点（A 源，中途真 SIGTERM）→ meta 有 done 且 etag 是旧值；
  B. 换 B 源（同 URL、同长度、不同内容、不同 ETag）跑普通命令 →
     rc≠0、文案自洽（说"已丢弃"，不再出现"重跑同命令可续传"），
     且 meta 里 done 已清空、etag 已是新源 —— 即"真的作废了"；
  C. 紧接着再跑一次普通命令 → rc=0，成品 sha256 == 源 B 的 sha256。

为什么同时断言 meta 内容而不只看 rc：只看 rc≠0 的话，"根本没作废"
（旧缺陷）和"作废了"（修好）表现完全一样 —— 必须看磁盘。

运行：python3 -m unittest tests.test_p01_discard_on_source_change -v
"""
import contextlib
import hashlib
import http.server
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

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, "fluxdl.py")

# 2 MiB：够慢到能在中途打断，又不至于让整套用例跑太久（限速 128KB/s → 约 16s 全长）
_BLK = 64 * 1024
SOURCE_A = (bytes(range(256)) * (_BLK // 256)) * 32          # 2 MiB
SOURCE_B = (bytes(range(255, -1, -1)) * (_BLK // 256)) * 32
ETAG_A = '"v-A"'
ETAG_B = '"v-B"'
assert len(SOURCE_A) == len(SOURCE_B) == 2 * 1024 * 1024
assert SOURCE_A != SOURCE_B


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class _Source:
    """可换内容的 Range 源站：ETag 由内容版本决定，与内容字节强绑定。

    刻意让 ETag 与"服务端此刻返回的 payload"同步变 —— 这就是 nginx
    覆盖同名文件后的现场（大小一个字节不差，ETag 变了）。
    """

    def __init__(self):
        self.payload = SOURCE_A
        self.etag = ETAG_A
        self.log = []          # (method, range, if_range, status)

    def set(self, payload: bytes, etag: str):
        self.payload, self.etag = payload, etag


def _make_handler(src: _Source):
    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            self._serve(head=True)

        def do_GET(self):
            self._serve(head=False)

        def _serve(self, head):
            data, etag = src.payload, src.etag
            total = len(data)
            rng = self.headers.get("Range")
            ir = self.headers.get("If-Range")
            # If-Range 不匹配 → RFC 7233 要求回 200 全量
            honor = not (ir and ir != etag)
            if rng and rng.startswith("bytes=") and honor:
                a, _, b = rng[6:].partition("-")
                start, end = int(a or 0), int(b or total - 1)
                end = min(end, total - 1)
                chunk, code = data[start:end + 1], 206
            else:
                chunk, code = data, 200
            src.log.append(("HEAD" if head else "GET", rng, ir, code))
            self.send_response(code)
            if code == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", etag)
            self.end_headers()
            if not head:
                # 限速 ~128KB/s：2 MiB 约 16s，保证能在中途 SIGTERM 留下断点
                for off in range(0, len(chunk), 16384):
                    try:
                        self.wfile.write(chunk[off:off + 16384])
                    except (BrokenPipeError, ConnectionResetError):
                        return
                    time.sleep(0.125)

        def log_message(self, *a):
            pass

    return H


class _Server:
    def __init__(self, handler):
        self.port = _free_port()
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), handler)

    def __enter__(self):
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        time.sleep(0.05)
        return "http://127.0.0.1:%d" % self.port

    def __exit__(self, *a):
        self.httpd.shutdown()
        self.httpd.server_close()


class TestDiscardOnSourceChange(unittest.TestCase):
    def setUp(self):
        self.src = _Source()
        self.td = tempfile.mkdtemp(prefix="fluxdl_r21_")
        self.addCleanup(shutil.rmtree, self.td, True)
        self.out = os.path.join(self.td, "blob.bin")
        self.meta = self.out + ".fluxdl"

    # ---- 公用：同 URL 上先真中断，再按需换内容 -------------------------------
    @contextlib.contextmanager
    def _interrupted(self, plant_b: bool):
        """必须在**同一 URL**上换内容：_load_meta 会校验 meta['url']，
        换端口就变成另一任务 → 检查点整体作废 → 走 fresh 全量下载，
        那么"续传分支有没有作废断点"这件事根本没被测到（假绿）。"""
        with _Server(_make_handler(self.src)) as base:
            p = subprocess.Popen(
                [sys.executable, CLI, base + "/blob.bin", "-o", self.td,
                 "-c", "1", "--no-verify"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deadline = time.time() + 15
            while time.time() < deadline:
                try:
                    with open(self.meta, encoding="utf-8") as f:
                        if json.load(f).get("done"):
                            break
                except Exception:
                    pass
                time.sleep(0.02)
            p.terminate()
            p.wait(timeout=15)
            self.assertEqual(p.returncode, -15, "应被 SIGTERM 打断")
            # 步骤 1 的断言：断点确实留在磁盘上，且指纹是 A
            with open(self.meta, encoding="utf-8") as f:
                m = json.load(f)
            self.assertTrue(m.get("done"), "SIGTERM 后应留下非空 done；实际 %r" % (m,))
            self.assertEqual(m.get("etag"), ETAG_A, "断点指纹应是旧源 ETag A")
            done_before = m["done"]
            if plant_b:
                self.src.set(SOURCE_B, ETAG_B)
            self.src.log = []
            yield base, done_before

    def _run_cli(self, base, timeout=90):
        """跑一次普通命令（不带 --force-restart），返回 (rc, stdout+stderr)。

        必须用「真的 -c 1 全速下完」而不是在主进程里调 main()：限速 128KB/s 时
        2 MiB 要 16s，与本文件里 1.3s 的探针共享同一个源站，光靠 io.StringIO
        抓输出会让两侧互相拖慢（首版就是这么误报 rc=0 的）。
        用同一个可执行文件、同样的参数，验收口径与人工实测完全一致。
        """
        r = subprocess.run(
            [sys.executable, CLI, base + "/blob.bin", "-o", self.td,
             "-c", "1", "--no-verify"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        return r.returncode, r.stdout.decode("utf-8", "replace")

    # ---- 主用例：三步 -------------------------------------------------------
    def test_discard_then_plain_rerun_succeeds(self):
        """B: 报错自洽且真的作废；C: 紧接着重跑一次普通命令就拿到新内容。"""
        with self._interrupted(plant_b=True) as (base, done_before):
            rc1, msg1 = self._run_cli(base)

            # ---- 步骤 2：源已变更 -------------------------------------------
            self.assertNotEqual(rc1, 0, "源已变更必须非 0 退出")
            self.assertIn("源已变更", msg1)
            self.assertIn(ETAG_A, msg1, "报错要给出旧指纹")
            self.assertIn(ETAG_B, msg1, "报错要给出新指纹")
            self.assertIn("丢弃", msg1, "要说明旧断点的处置")
            # 核心文案断言：不许同时说"已作废/已丢弃"和"重跑同命令可续传"
            self.assertNotIn("重跑同命令可续传", msg1,
                             "源变更分支不得出现通用的半成品提示（自相矛盾）")
            self.assertIn("重跑同一条命令", msg1, "要给出正确的下一步")

            # 核心行为断言：断点必须已经真的作废
            with open(self.meta, encoding="utf-8") as f:
                m = json.load(f)
            self.assertEqual(m.get("done"), [],
                             "报错时旧断点必须真的作废（done 清空）；"
                             "实际仍是 %r（= 缺陷未修）" % (m.get("done"),))
            self.assertNotEqual(m.get("etag"), ETAG_A,
                                "meta 里不该再留着旧源指纹")
            self.assertEqual(m.get("etag"), ETAG_B,
                             "meta 指纹应更新为新源，下一轮才不会再撞一次源已变更")

            # ---- 步骤 3：紧接着再跑一次普通命令 -----------------------------
            rc2, msg2 = self._run_cli(base)
            self.assertEqual(rc2, 0,
                             "作废之后再跑普通命令必须成功；msg=%r" % (msg2,))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_B),
                             "成品必须是新源 B，不能是旧数据或新旧混合")
            self.assertFalse(os.path.exists(self.meta),
                             "下完后检查点应被清理")

    def test_one_plain_rerun_is_enough(self):
        """「重跑同一条命令」必须一次就够 —— 这是本缺陷的直接判据。

        旧行为是：作废只写在屏幕上，磁盘上 done 还在 → 第二次、第三次……
        每次都报同一句，永远出不来。这里断言第二次运行就是成功，
        且成品等于新源（而不是"又失败一次也算过"）。
        """
        with self._interrupted(plant_b=True) as (base, _done_before):
            rc1, _ = self._run_cli(base)
            self.assertEqual(rc1, 1, "第一次应当是明确的源已变更失败")
            rc2, msg2 = self._run_cli(base)
            self.assertEqual(rc2, 0,
                             "第二次普通命令必须成功（作废要真的生效），msg=%r" % (msg2,))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_B),
                             "成品必须是新源 B")
            # 若这里仍看到 done 被写回旧进度，说明 _flush_meta 又把 pend 合并回来了
            self.assertFalse(os.path.exists(self.meta),
                             "成功收尾后不应残留检查点")

    def test_unchanged_source_still_resumes(self):
        """对照组：源没变时不能因为这次改动就丢掉断点（否则正常续传被误伤）。"""
        with self._interrupted(plant_b=False) as (base, done_before):
            rc, msg = self._run_cli(base)
            self.assertEqual(rc, 0, "源未变必须续传成功，msg=%r" % (msg,))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_A))
            # 续传而不是重下：至少有一次带 If-Range 的数据 GET
            resuming = [r for r in self.src.log if r[0] == "GET" and r[1]]
            self.assertTrue(resuming, "续传阶段应当发出过带 Range 的 GET")
            self.assertTrue(any(r[2] for r in resuming),
                            "续传请求应至少带着 If-Range；实际 %r" % (resuming[:3],))


if __name__ == "__main__":
    unittest.main(verbosity=2)

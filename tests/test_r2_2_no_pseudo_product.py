"""R2-2 回归：rc≠0 退出后不许在成品名上留下「伪成品」（任务书 R2-2）。

缺陷现场（外部黑盒脚本实测，非本目录用例）：
    A 源 4 MiB / 300KB/s 跑 4s 后 SIGTERM → rc=-15，meta done=[[0,1048576]] etag="v-A"；
    换成 B 源（同 URL、同长度、不同内容、etag="v-B"）普通重跑 → rc=1，0.08s，
    文案「源已变更…」正确，meta 也真的作废了（done=[] etag="v-B"）——
    但退出后目录里躺着：
        blob.bin 4194304 B ← 前 1 MiB 是旧源 A 的数据，其余是预分配零填充
        blob.bin.lock 7 B
    4 MiB 正好是**最终大小**，名字也正好是**最终名字**。任何外部脚本
    （`test -f`、比 size、`sha256sum` 之外的存在性检查）都会把它当成品用。

本文件锁死方案 A 的行为（下载中的正文一律写 blob.bin.part，成功后 os.replace）：
  (a) SIGTERM 中断后：成品名不存在（或大小 ≠ 最终大小）；*.part 存在且内容 ≤ 已下字节；
  (b) 换源导致 rc≠0 后：不存在「成品名 + 最终大小」的文件；
  (c) 正常跑完 rc=0：成品存在、sha256 正确、无 .part 残留；
  (d) 续传路径：发过续传请求（偏移 ≥ 旧 done 末尾）、成品 sha256 == 源；
  (e) 另附锁语义与 --force-restart 的实测，确保没被这次改名破坏。

为什么全部真起服务 + 真跑 CLI 子进程：伪成品的危害只在「进程真的退出、
别人来读目录」这一点上体现。在进程内调 main() 会因为同一个 PID 复用
文件句柄/时序，测不出"退出后目录长什么样"。

运行：python3 -m unittest discover -s tests -q
     或 python3 -m unittest tests/test_r2_2_no_pseudo_product.py -v
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

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, "fluxdl.py")
sys.path.insert(0, REPO)

MiB = 1024 * 1024
TOTAL = 4 * MiB              # 与缺陷现场同尺寸
# 两份内容大小完全一致、内容完全不同 —— 现场就是 nginx 覆盖同名文件
SOURCE_A = (bytes(range(256)) * (MiB // 256)) * 4
SOURCE_B = (bytes(range(255, -1, -1)) * (MiB // 256)) * 4
ETAG_A = '"v-A"'
ETAG_B = '"v-B"'
assert len(SOURCE_A) == len(SOURCE_B) == TOTAL
assert SOURCE_A != SOURCE_B

# 300 KB/s ≈ 缺陷现场的限速：4 MiB 要 14s，>1s 就够在中途 SIGTERM
RATE = 300 * 1024
STEP = 32 * 1024


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
    """可换内容 + 可限速的 Range 源站；ETag 与内容版本强绑定。"""

    def __init__(self):
        self.payload = SOURCE_A
        self.etag = ETAG_A
        self.log = []              # (method, range, if_range, status)
        self.slow = True           # 中断用：限速；全量下载用例会关掉

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
            honor = not (ir and ir != etag)      # If-Range 不匹配 → 回 200 全量
            if rng and rng.startswith("bytes=") and honor:
                a, _, b = rng[len("bytes="):].partition("-")
                a = int(a or 0)
                b = int(b) if b else total - 1
                b = min(b, total - 1)
                chunk = data[a:b + 1]
                src.log.append(("GET" if not head else "HEAD", rng, ir, 206))
                self.send_response(206)
                self.send_header("Content-Range", "bytes %d-%d/%d" % (a, b, total))
                self.send_header("Content-Length", str(len(chunk)))
            else:
                chunk = data
                src.log.append(("GET" if not head else "HEAD", rng, ir, 200))
                self.send_response(200)
                self.send_header("Content-Length", str(total))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", etag)
            self.send_header("Content-Type", "application/octet-stream")
            self.end_headers()
            if head:
                return
            if not src.slow:
                self.wfile.write(chunk)
                return
            # 限速：不限速的话回环上 4 MiB 几十毫秒就下完，来不及中途打断
            # BrokenPipe 是 SIGTERM 场景的必然产物（客户端被杀，服务端还在写
            # 当前块）；它不是被测行为，夹在输出里只会淹没真正的失败信息。
            try:
                for off in range(0, len(chunk), STEP):
                    self.wfile.write(chunk[off:off + STEP])
                    time.sleep(STEP / RATE)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *a):
            pass

    return H


class _Server:
    def __init__(self, handler):
        self.port = _free_port()
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", self.port), handler)
        self.httpd.daemon_threads = True

    def __enter__(self):
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        time.sleep(0.05)
        return "http://127.0.0.1:%d" % self.port

    def __exit__(self, *a):
        self.httpd.shutdown()
        self.httpd.server_close()


class TestNoPseudoProduct(unittest.TestCase):
    def setUp(self):
        self.src = _Source()
        self.td = tempfile.mkdtemp(prefix="fluxdl_r22_")
        self.addCleanup(shutil.rmtree, self.td, True)
        self.out = os.path.join(self.td, "blob.bin")
        self.part = self.out + ".part"
        self.meta = self.out + ".fluxdl"

    # ---- 公用 ---------------------------------------------------------------
    def _cli(self, base, *extra, timeout=120):
        return subprocess.run(
            [sys.executable, CLI, base + "/blob.bin", "-o", self.td, "-c", "1", *extra],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)

    @contextlib.contextmanager
    def _interrupted(self, plant_b=False):
        """同一个 URL 上先真 SIGTERM 造断点，再按需换内容。"""
        with _Server(_make_handler(self.src)) as base:
            self.src.payload, self.src.etag = SOURCE_A, ETAG_A
            self.src.slow = True
            p = subprocess.Popen(
                [sys.executable, CLI, base + "/blob.bin", "-o", self.td, "-c", "1"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            # 等到 meta 里出现完成区间（说明确实写进磁盘了），再打断
            deadline = time.time() + 60
            while time.time() < deadline:
                try:
                    with open(self.meta, encoding="utf-8") as f:
                        if json.load(f).get("done"):
                            break
                except Exception:
                    pass
                time.sleep(0.02)
            p.terminate()
            p.wait(timeout=20)
            self.assertEqual(p.returncode, -15, "应被 SIGTERM 打断，实际 %r" % p.returncode)
            with open(self.meta, encoding="utf-8") as f:
                done_before = json.load(f)["done"]
            if plant_b:
                self.src.set(SOURCE_B, ETAG_B)
            self.src.log = []
            self.src.slow = False       # 后续全量下载不限速，用例跑得快
            yield base, done_before

    def _files(self):
        return sorted(os.listdir(self.td))

    # ---- (a) SIGTERM 中断后 -------------------------------------------------
    def test_a_sigterm_leaves_no_product_name(self):
        """中断后：成品名不得存在（更不能是最终大小）；.part 在，且内容 ≤ 已下字节。"""
        with self._interrupted() as (base, done):
            done_bytes = sum(b - a for a, b in done)
            self.assertGreater(done_bytes, 0)
            # 核心断言：成品名绝不能以"最终大小"出现
            if os.path.exists(self.out):
                self.assertNotEqual(
                    os.path.getsize(self.out), TOTAL,
                    "SIGTERM 后成品名不得是最终大小（这就是伪成品）：files=%r" % (self._files(),))
            # .part 必须存在，保存着已下数据
            self.assertTrue(os.path.exists(self.part),
                            "中断后应有 .part 承接半成品；实际目录 %r" % (self._files(),))
            part_size = os.path.getsize(self.part)
            self.assertLessEqual(part_size, TOTAL,
                                 ".part 不得超过最终大小")
            self.assertGreaterEqual(part_size, min(done_bytes, 1),
                                    ".part 至少覆盖已确认的字节")
            # 半成品里真的有 A 源数据（前 done_bytes 字节）
            with open(self.part, "rb") as f:
                head = f.read(min(done_bytes, 256 * 1024))
            self.assertEqual(head, SOURCE_A[:len(head)],
                             ".part 前缀应是已下载的源数据")
            # 元数据自洽
            with open(self.meta, encoding="utf-8") as f:
                m = json.load(f)
            self.assertEqual(m.get("etag"), ETAG_A)
            self.assertTrue(self._files())

    # ---- (b) 换源 rc≠0 后 --------------------------------------------------
    def test_b_source_change_leaves_no_final_sized_file(self):
        """换源 → rc≠0：目录里不存在「成品名 + 最终大小」的文件。"""
        with self._interrupted(plant_b=True) as (base, _done):
            r = self._cli(base)
            rc, msg = r.returncode, r.stdout.decode("utf-8", "replace")
            self.assertEqual(rc, 1, "源已变更应 rc=1；实际 %r，输出 %r" % (rc, msg))
            self.assertIn("源已变更", msg)

            files = self._files()
            # 本轮的验收核心：任何"名字像成品"的文件都不得是最终大小
            bad = []
            for name in files:
                path = os.path.join(self.td, name)
                if not os.path.isfile(path):
                    continue
                size = os.path.getsize(path)
                if name == "blob.bin" and size == TOTAL:
                    bad.append((name, size))
                # 更严格：除 .part / meta / lock 之外的裸成品名都不允许是最终大小
                if name not in ("blob.bin.part", "blob.bin.fluxdl",
                                "blob.bin.lock") and name.startswith("blob.bin") and size == TOTAL:
                    bad.append((name, size))
            self.assertEqual(bad, [], "rc≠0 后不得留下「成品名 + 最终大小」的文件：%r" % (bad,))
            self.assertFalse(os.path.exists(self.out) and os.path.getsize(self.out) == TOTAL,
                             "blob.bin 不得以最终大小存在；目录 %r" % (files,))
            # meta 已真的作废（R2-1 行为不变）
            with open(self.meta, encoding="utf-8") as f:
                m = json.load(f)
            self.assertEqual(m.get("done"), [], "换源后 meta 的 done 必须已清空")
            self.assertEqual(m.get("etag"), ETAG_B)

            # R2-1 不变：紧接着再跑一次 rc=0 且成品 == 新源
            r2 = self._cli(base)
            msg2 = r2.stdout.decode("utf-8", "replace")
            self.assertEqual(r2.returncode, 0, "换源后的下一轮应成功；输出 %r" % (msg2,))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_B))
            self.assertFalse(os.path.exists(self.part), "成功后不得留下 .part")

    # ---- (c) rc=0 干净完成 --------------------------------------------------
    def test_c_clean_run_has_no_part_leftover(self):
        """正常跑完：成品存在、sha256 正确、目录里没有 .part。"""
        with _Server(_make_handler(self.src)) as base:
            self.src.slow = False
            r = self._cli(base)
            msg = r.stdout.decode("utf-8", "replace")
            self.assertEqual(r.returncode, 0, "正常下载应 rc=0；输出 %r" % (msg,))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_A))
            self.assertEqual(os.path.getsize(self.out), TOTAL)
            self.assertFalse(os.path.exists(self.part),
                             "rc=0 时不得残留 .part；目录 %r" % (self._files(),))
            # 既有行为：成功后 meta 被删，只剩成品 + 锁文件
            self.assertFalse(os.path.exists(self.meta), "成功后应删除 .fluxdl")
            self.assertNotIn("blob.bin.part", self._files())

    # ---- (d) 续传路径 ------------------------------------------------------
    def test_d_resume_requests_and_result(self):
        """续传：同源重跑 rc=0，续传请求偏移 ≥ 旧 done 末尾，成品 sha == 源。"""
        with self._interrupted() as (base, done):
            end = max(b for _a, b in done)
            r = self._cli(base)
            msg = r.stdout.decode("utf-8", "replace")
            self.assertEqual(r.returncode, 0, "同源重跑应续传成功；输出 %r" % (msg,))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_A),
                             "续传结果必须与源完全一致")
            # 真的只下了剩下那段：存在偏移 ≥ 旧 done 末尾的数据 GET
            rngs = []
            for method, rng, _ir, status in self.src.log:
                if method != "GET" or not rng or status != 206:
                    continue
                a, _, b = rng[len("bytes="):].partition("-")
                rngs.append(int(a or 0))
            self.assertTrue(rngs, "续传阶段应发出过带 Range 的 GET；log=%r"
                            % (self.src.log[:5],))
            self.assertTrue(any(off >= end for off in rngs),
                            "应有偏移 ≥ 已下末尾(%d) 的续传请求；实际 %r" % (end, rngs[:8]))
            self.assertFalse(os.path.exists(self.part))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_A))

    # ---- (e) --force-restart 仍然清干净 ------------------------------------
    def test_e_force_restart_cleans_part_and_meta(self):
        """.part 与 meta 都被清掉，从零开始。"""
        with self._interrupted() as (base, _done):
            self.assertTrue(os.path.exists(self.part), "前置：应存在 .part")
            r = self._cli(base, "--force-restart")
            msg = r.stdout.decode("utf-8", "replace")
            self.assertEqual(r.returncode, 0, "--force-restart 应成功；输出 %r" % (msg,))
            self.assertEqual(_sha_file(self.out), _sha(SOURCE_A))
            self.assertFalse(os.path.exists(self.part))
            self.assertFalse(os.path.exists(self.meta))
            # 从零开始：第一个 206 请求的偏移必须是 0
            offs = [int(rng[len("bytes="):].split("-")[0])
                    for m, rng, _ir, st in self.src.log
                    if m == "GET" and rng and st == 206]
            self.assertIn(0, offs, "--force-restart 必须从偏移 0 重新开始；实际 %r" % (offs[:8],))

    # ---- 锁语义不变 --------------------------------------------------------
    def test_lock_still_blocks_second_process(self):
        """第二个进程同输出名必须明确报错退出，不能出现 A 完成后 B 覆盖。"""
        with _Server(_make_handler(self.src)) as base:
            self.src.slow = True
            p1 = subprocess.Popen(
                [sys.executable, CLI, base + "/blob.bin", "-o", self.td, "-c", "1"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            try:
                # 等 p1 真正拿到锁（锁文件里有 PID）
                deadline = time.time() + 30
                locked = False
                while time.time() < deadline:
                    if os.path.exists(self.out + ".lock"):
                        try:
                            if open(self.out + ".lock").read().strip():
                                locked = True
                                break
                        except Exception:
                            pass
                    time.sleep(0.02)
                self.assertTrue(locked, "A 应先拿到锁")
                r2 = subprocess.run(
                    [sys.executable, CLI, base + "/blob.bin", "-o", self.td, "-c", "1"],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
                msg2 = r2.stdout.decode("utf-8", "replace")
                self.assertNotEqual(r2.returncode, 0, "第二个进程必须失败退出，不能复用同一把锁")
                self.assertIn("锁", msg2, "报错要说清是锁冲突；实际 %r" % (msg2,))
            finally:
                p1.terminate()
                p1.wait(timeout=20)
            # A 被打断后成品名不得是最终大小（同样的伪成品判据）
            if os.path.exists(self.out):
                self.assertNotEqual(os.path.getsize(self.out), TOTAL,
                                    "被 SIGTERM 的 A 不得留下最终大小的成品名")


if __name__ == "__main__":
    unittest.main(verbosity=2)

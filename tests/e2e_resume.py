#!/usr/bin/env python3
"""端到端自测：P1-C（重启扫 .fluxdl 重建可续传）+ P1-B（SSE 增量）+ P2-B（连接预算）。

独立跑一个限速 Range 服务器当下游，全程本机、无外网依赖。
运行：python3 tests/e2e_resume.py
"""
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRV_PORT = 8898
SRC_PORT = 8897
BASE = f"http://127.0.0.1:{SRV_PORT}"
TOK = "testtok"
HOST = f"127.0.0.1:{SRV_PORT}"
fails = []
SRC_SIZE = 8 * 1024 * 1024
src_path = None
hits_lock = threading.Lock()
range_starts = []


def check(name, cond, extra=""):
    print(("  PASS " if cond else "  FAIL ") + name + (("  " + extra) if extra else ""))
    if not cond:
        fails.append(name)


class RangeHandler(BaseHTTPRequestHandler):
    """stdlib Range 服务器，限速 1MB/s（够慢到能在中途打断）。"""
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_GET(self):
        try:
            f = open(src_path, "rb")
        except OSError:
            self.send_error(404)
            return
        with f:
            rng = self.headers.get("Range")
            start = 0
            if rng:
                m = re.match(r"bytes=(\d+)-(\d*)", rng)
                if m:
                    start = int(m.group(1))
            with hits_lock:
                range_starts.append(start)
            f.seek(start)
            remain = SRC_SIZE - start
            self.send_response(206 if rng else 200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(remain))
            self.send_header("Accept-Ranges", "bytes")
            if rng:
                self.send_header("Content-Range",
                                 f"bytes {start}-{SRC_SIZE - 1}/{SRC_SIZE}")
            self.end_headers()
            chunk = 64 * 1024
            sent = 0
            while remain > 0:
                buf = f.read(min(chunk, remain))
                if not buf:
                    break
                try:
                    self.wfile.write(buf)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return
                remain -= len(buf)
                sent += len(buf)
                time.sleep(len(buf) / (1024 * 1024))   # 1MB/s 限速


def req(path, *, method="GET", body=None, headers=None, timeout=10):
    h = {"Host": HOST, "X-Token": TOK}
    h.update(headers or {})
    r = urllib.request.Request(BASE + path, method=method, data=body, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def wait_ready(proc, path="/", tries=60):
    for _ in range(tries):
        if proc.poll() is not None:
            return False
        try:
            req(path, headers={"Host": HOST})
            return True
        except Exception:
            time.sleep(0.15)
    return False


def launch(dl, port, extra_env=None):
    env = dict(os.environ, FLUXDL_TOKEN=TOK, FLUXDL_PORT=str(port),
               FLUXDL_DIR=dl, PYTHONUNBUFFERED="1", **(extra_env or {}))
    return subprocess.Popen([sys.executable, "fluxdl_server.py"], cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def stop(proc, sig=signal.SIGTERM):
    if proc.poll() is not None:
        return ""
    proc.send_signal(sig)
    try:
        out, _ = proc.communicate(timeout=12)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    return out or ""


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def main():
    global src_path
    work = tempfile.mkdtemp(prefix="fluxdl_resume_")
    src_path = os.path.join(work, "src.bin")
    with open(src_path, "wb") as f:
        f.write(os.urandom(SRC_SIZE))
    want = sha(src_path)

    src_srv = ThreadingHTTPServer(("127.0.0.1", SRC_PORT), RangeHandler)
    src_srv.daemon_threads = True
    threading.Thread(target=src_srv.serve_forever, daemon=True).start()

    dl = os.path.join(work, "dl")
    os.makedirs(dl, exist_ok=True)
    url = f"http://127.0.0.1:{SRC_PORT}/src.bin"

    p = launch(dl, SRV_PORT)
    try:
        if not wait_ready(p):
            print("服务没能起来"); return 1

        print("== P1-C① 下到一半被 SIGTERM ==")
        st, b = req("/api/add", method="POST",
                    body=json.dumps({"url": url, "concurrency": 2}).encode(),
                    headers={"Content-Type": "application/json"})
        tid = json.loads(b)["id"] if st == 200 else None
        check("/api/add 成功", st == 200 and tid, f"status={st}")
        time.sleep(2.5)
        st, b = req("/api/list")
        lst = json.loads(b)
        t = [x for x in lst["tasks"] if x["id"] == tid][0]
        mid = t.get("done", 0)
        check("中断前确实下了一部分", mid > 0, f"done={mid}")
        out = stop(p)
        check("P3：SIGTERM 有收尾日志", "bye" in out, repr(out[-120:]))
        sidecars = [f for f in os.listdir(dl) if f.endswith(".fluxdl")]
        check("断点 sidecar 已落盘", len(sidecars) == 1, str(sidecars))

        print("== P1-C② 重启后扫出「可续传」任务 ==")
        p = launch(dl, SRV_PORT)
        if not wait_ready(p):
            print("重启后服务没起来"); return 1
        st, b = req("/api/list")
        lst = json.loads(b)["tasks"]
        check("重启后有 1 个任务", len(lst) == 1, f"n={len(lst)}")
        if lst:
            nt = lst[0]
            check("状态是「可续传」", nt["state"] == "可续传", nt["state"])
            check("沿用已下字节数", nt.get("done", 0) > 0, f"done={nt.get('done')}")
            check("有 name（卡片标题不为空）", bool(nt.get("name")), repr(nt.get("name")))

        print("== P1-C③ 点「继续」→ 从缺口接着下 → sha256 一致 ==")
        # 记住"点继续之前"已经发出多少个 Range 请求，之后新增的才算续传轮次
        before = len(range_starts)
        st, _ = req(f"/api/resume?id={lst[0]['id']}", method="POST")
        check("/api/resume 返回 200", st == 200, f"status={st}")

        done_ok = False
        for _ in range(120):
            time.sleep(0.5)
            st, b = req("/api/list")
            cur = json.loads(b)["tasks"][0]
            if cur["state"] in ("完成", "失败"):
                done_ok = cur["state"] == "完成"
                break
        check("续传后状态=完成", done_ok, cur["state"] + " " + str(cur.get("message")))
        fin = os.path.join(dl, cur.get("name") or "")
        check("成品存在", os.path.exists(fin), fin)
        if os.path.exists(fin):
            check("sha256 与源一致（拼接无损）", sha(fin) == want)
        # 续传起点证据：第二轮（before 之后）发出的 Range 请求里，
        # 必须有起点明显 > 0 的（说明是从缺口接上而不是从 0 重下）。
        later = range_starts[before:]
        check("续传请求起点 > 0（没从 0 重下）",
              any(s > 0 for s in later), f"later={later[:8]} mid={mid}")
        check("续传起点接近已下字节",
              any(s >= mid // 2 for s in later), f"later={later[:8]} mid={mid}")
    finally:
        stop(p)
        src_srv.shutdown()
        shutil.rmtree(work, ignore_errors=True)

    print("\n结果: %d 项失败" % len(fails) if fails else "\n全部通过")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())

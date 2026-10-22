#!/usr/bin/env python3
"""D1 实测（严格版）：限速源 + 真在途下载，SIGTERM 断言 10s 内退出且 rc==0。

要点：必须真的处于「下载中」才发信号，否则测的是空闲退出，掩盖死锁。
"""
import json, os, shutil, signal, socket, subprocess, sys, threading, time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 8922
SRC_PORT = 8912
DL = "/tmp/fdl_d1dl/dl"
RATE = 2_000_000          # 2 MB/s，60MB → 至少 30s，绝对来不及跑完
BASE = f"http://127.0.0.1:{PORT}"
TOK = "tokd1b"

# ---- 限速源（stdlib only） ----
class Slow(http.server.BaseHTTPRequestHandler if False else object):
    pass

import http.server

SRC = "/tmp/fdl_d1dl/src/big60.bin"

class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def do_GET(self):
        size = os.path.getsize(SRC)
        rng = self.headers.get("Range")
        start, end = 0, size - 1
        if rng and rng.startswith("bytes="):
            a, _, b = rng[6:].partition("-")
            start = int(a or 0)
            end = int(b) if b else size - 1
            end = min(end, size - 1)
        self.send_response(206 if rng else 200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        if rng:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(end - start + 1))
        self.end_headers()
        chunk = 65536
        sent = 0
        with open(SRC, "rb") as f:
            f.seek(start)
            while sent < end - start + 1:
                n = min(chunk, end - start + 1 - sent)
                buf = f.read(n)
                if not buf:
                    break
                try:
                    self.wfile.write(buf)
                except OSError:
                    return
                sent += n
                time.sleep(n / RATE)

srv = http.server.ThreadingHTTPServer(("127.0.0.1", SRC_PORT), H)
srv.daemon_threads = True
threading.Thread(target=srv.serve_forever, daemon=True).start()

shutil.rmtree(DL, ignore_errors=True)
os.makedirs(DL, exist_ok=True)
LOG = "/tmp/fdl_d1dl/serve.log"
env = dict(os.environ, FLUXDL_TOKEN=TOK, FLUXDL_PORT=str(PORT),
           FLUXDL_DIR=DL, PYTHONUNBUFFERED="1")
logf = open(LOG, "w")
p = subprocess.Popen([sys.executable, "fluxdl_server.py"], cwd=ROOT, env=env,
                     stdout=logf, stderr=subprocess.STDOUT)


def api(path, method="GET", body=None):
    h = {"Host": f"127.0.0.1:{PORT}", "X-Token": TOK}
    if body is not None:
        h["Content-Type"] = "application/json"
    r = urllib.request.Request(BASE + path, method=method, headers=h,
                               data=json.dumps(body).encode() if body else None)
    with urllib.request.urlopen(r, timeout=10) as resp:
        return json.loads(resp.read())


def tasks():
    lst = api("/api/list")
    return lst.get("tasks", lst if isinstance(lst, list) else [])


ok = False
try:
    for _ in range(80):
        try:
            api("/api/list"); break
        except Exception:
            time.sleep(0.1)

    api("/api/add", "POST", {"url": f"http://127.0.0.1:{SRC_PORT}/big60.bin",
                             "concurrency": 8})
    # 等到确实「下载中」且已有进度
    deadline = time.time() + 15
    while time.time() < deadline:
        ts = tasks()
        if ts and ts[0]["state"] == "下载中" and (ts[0].get("done") or 0) > 0:
            break
        time.sleep(0.3)
    ts = tasks()
    print("SIGTERM 前:", [(t["id"], t["state"], t.get("done"), t.get("percent")) for t in ts])
    if not ts or ts[0]["state"] != "下载中":
        print("FAIL: 没能在发信号前进入「下载中」，用例无效")
        sys.exit(1)

    t0 = time.time()
    p.send_signal(signal.SIGTERM)
    try:
        rc = p.wait(timeout=10)
        elapsed = time.time() - t0
    except subprocess.TimeoutExpired:
        print("FAIL: SIGTERM 后 10s 未退出（D1 死锁仍在）")
        p.kill(); p.wait()
        sys.exit(1)

    print(f"SIGTERM → 退出耗时 {elapsed:.2f}s, returncode={rc}")
    ok = (rc == 0 and elapsed < 10)
    print("D1 断言：下载中收到 SIGTERM，10s 内退出且 rc==0 →",
          "PASS" if ok else "FAIL")
    metas = sorted(n for n in os.listdir(DL) if n.endswith(".fluxdl"))
    print("断点文件:", metas)
finally:
    srv.shutdown()
    logf.close()
    print("---- 服务端日志 ----")
    print(open(LOG).read().strip()[-900:])
    print("--------------------")
    sys.exit(0 if ok else 1)

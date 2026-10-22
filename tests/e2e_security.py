#!/usr/bin/env python3
"""端到端自测：P1-A 安全项（Host 白名单 + 不再内联 token + 一次性 nonce）。

用前台子进程起服务（不继承 SIG_IGN 的问题只在测信号时才有，这里只测 HTTP），
固定 FLUXDL_TOKEN=testtok / FLUXDL_PORT / FLUXDL_DIR。

运行：python3 tests/e2e_security.py
"""
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 8899
BASE = f"http://127.0.0.1:{PORT}"
TOK = "testtok"
fails = []


def check(name, cond, extra=""):
    print(("  PASS " if cond else "  FAIL ") + name + (("  " + extra) if extra else ""))
    if not cond:
        fails.append(name)


def req(path, *, method="GET", headers=None, body=None):
    r = urllib.request.Request(BASE + path, method=method, data=body,
                               headers=headers or {})
    try:
        with urllib.request.urlopen(r, timeout=5) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def sse_status(path):
    """探 SSE 长连接只看【响应头行】，然后立刻断开。

    坑：SSE 永远不会结束，urllib.request.urlopen().read() 会一直阻塞到
    timeout，早期版本这里让整个 e2e 挂死（还把端口占着，下次跑直接
    Address already in use）。所以用裸 socket 读第一行 status line。
    返回 (status_code, realtime_header_present)。
    """
    try:
        s = socket.create_connection(("127.0.0.1", PORT), timeout=5)
    except OSError:
        return -1, False
    try:
        s.sendall((f"GET {path} HTTP/1.1\r\n"
                   f"Host: 127.0.0.1:{PORT}\r\n"
                   "Accept: text/event-stream\r\n"
                   "Connection: close\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
        head = buf.decode("latin-1", "replace")
        line = head.split("\r\n", 1)[0]
        parts = line.split()
        code = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else -1
        return code, ("text/event-stream" in head)
    except OSError:
        return -1, False
    finally:
        try:
            s.close()
        except OSError:
            pass


def main():
    dl = tempfile.mkdtemp(prefix="fluxdl_srvtest_")
    env = dict(os.environ, FLUXDL_TOKEN=TOK, FLUXDL_PORT=str(PORT),
               FLUXDL_DIR=dl, PYTHONUNBUFFERED="1")
    p = subprocess.Popen([sys.executable, "fluxdl_server.py"], cwd=ROOT, env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        for _ in range(50):
            try:
                req("/", headers={"Host": f"127.0.0.1:{PORT}"})
                break
            except Exception:
                time.sleep(0.1)

        print("== P1-A① 页面不再内联 token ==")
        st, body = req("/", headers={"Host": f"127.0.0.1:{PORT}"})
        txt = body.decode()
        check("GET / 仍可打开（静态页）", st == 200, f"status={st}")
        check("页面不含 token 明文", TOK not in txt, f"len={len(txt)}")
        check("页面不含 __TOKEN__ 占位符", "__TOKEN__" not in txt)
        check("页面不含下载目录（路径泄露）", dl not in txt)

        print("== P1-A① Host 白名单 ==")
        st, _ = req("/", headers={"Host": f"evil.com:{PORT}"})
        check("Host: evil.com → 403", st == 403, f"status={st}")
        st, _ = req("/", headers={"Host": f"localhost:{PORT}"})
        check("Host: localhost → 200", st == 200, f"status={st}")
        st, _ = req("/api/list", headers={"Host": f"evil.com:{PORT}", "X-Token": TOK})
        check("Host 非法时即使带对 token 也 403", st == 403, f"status={st}")

        print("== P1-A② 鉴权仍然有效 ==")
        st, _ = req("/api/list", headers={"Host": f"127.0.0.1:{PORT}"})
        check("无 token 调 /api/list → 403", st == 403, f"status={st}")
        st, b = req("/api/list", headers={"Host": f"127.0.0.1:{PORT}", "X-Token": TOK})
        check("带 token 调 /api/list → 200", st == 200, f"status={st}")
        check("list 里带 dir（鉴权后才给目录）", dl.encode() in b)

        print("== P1-A② SSE nonce ==")
        st, _ = req("/api/nonce", method="POST", headers={"Host": f"127.0.0.1:{PORT}"})
        check("无 token 拿 nonce → 403", st == 403, f"status={st}")
        st, b = req("/api/nonce", method="POST",
                    headers={"Host": f"127.0.0.1:{PORT}", "X-Token": TOK})
        nonce = json.loads(b)["nonce"] if st == 200 else ""
        check("带 token 拿到 nonce", st == 200 and len(nonce) > 10, f"status={st}")

        st, is_sse = sse_status("/api/events?nonce=" + nonce)
        check("nonce 首次可用（SSE 200 + text/event-stream）",
              st == 200 and is_sse, f"status={st} sse={is_sse}")
        st2, _ = sse_status("/api/events?nonce=" + nonce)
        check("nonce 二次使用被拒（一次性）", st2 == 403, f"status={st2}")

        print("== 旧接口兼容：query 里的 token 不再被接受 ==")
        st, _ = req("/api/list?token=" + TOK, headers={"Host": f"127.0.0.1:{PORT}"})
        check("/api/list?token= → 403", st == 403, f"status={st}")

        print("== P1-B/P1-C/P3 冒烟 ==")
        st, b = req("/api/add", method="POST",
                    headers={"Host": f"127.0.0.1:{PORT}", "X-Token": TOK,
                             "Content-Type": "application/json"},
                    body=json.dumps({"url": "http://127.0.0.1:1/none.bin"}).encode())
        check("/api/add 可用（返回 task id）", st == 200 and b"id" in b, f"status={st}")
    finally:
        p.send_signal(signal.SIGTERM)
        try:
            out, _ = p.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
            out, _ = p.communicate()
        print("---- 服务端输出 ----")
        print((out or "").strip()[:800])
        print("--------------------")
        print("P3：SIGTERM 后进程应已退出 →", "OK" if p.poll() is not None else "仍在运行")
        shutil.rmtree(dl, ignore_errors=True)

    print("\n结果: %d 项失败" % len(fails) if fails else "\n全部通过")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())

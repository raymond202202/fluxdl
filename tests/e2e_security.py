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


def _free_port(start=8899, tries=200):
    """挑一个真正空闲的端口。

    坑（真实踩过两次）：写死 8899/8901 时，上一次跑挂的进程或环境里别的
    测试服务器会占着端口，服务起不来 → 后面的断言全都失败在"连不上/日志是
    错误栈"上，看起来像产品 bug，其实是测试环境脏。宁可多试几个端口。
    """
    for off in range(tries):
        port = start + off
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", port))
            except OSError:
                continue
        return port
    raise RuntimeError("找不到空闲端口")


PORT = _free_port()
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
        # ---- D1：SIGTERM 必须【自己】退出。禁止 SIGKILL 收尾 ----
        # 上一版这里只检查日志文本，超时就直接 p.kill()，于是
        # "shutdown() 在主线程自等死锁"被彻底掩盖（进程挂死 20s+ 仍判定 PASS）。
        # 现在断言退出的【事实】与【耗时】。
        t0 = time.time()
        p.send_signal(signal.SIGTERM)
        exited = True
        try:
            rc = p.wait(timeout=10)
            elapsed = time.time() - t0
        except subprocess.TimeoutExpired:
            exited = False
            rc = None
            elapsed = time.time() - t0
            p.kill()                      # 仅在断言失败后清理，不再是"正常收尾"
            p.wait()
        out = ""
        try:
            if p.stdout:
                out = p.stdout.read() or ""
        except Exception:
            pass
        print("---- 服务端输出 ----")
        print(out.strip()[:800])
        print("--------------------")
        check("D1：SIGTERM 后 10s 内自行退出", exited, f"elapsed={elapsed:.2f}s")
        check("D1：SIGTERM 退出码 == 0", rc == 0, f"rc={rc}")
        print(f"    实测退出耗时 {elapsed:.2f}s")

        # ---- D2：文件重定向 + 不设 PYTHONUNBUFFERED，1s 内日志必须有 token ----
        print("== D2 非 tty 日志立即可见 ==")
        dl2 = tempfile.mkdtemp(prefix="fluxdl_d2_")
        logf = os.path.join(dl2, "srv.log")
        env2 = dict(os.environ, FLUXDL_TOKEN="tokd2", FLUXDL_PORT=str(_free_port(PORT + 1)),
                    FLUXDL_DIR=dl2)
        env2.pop("PYTHONUNBUFFERED", None)      # 关键：模拟 systemd/docker/重定向
        with open(logf, "w") as fh:
            p2 = subprocess.Popen([sys.executable, "fluxdl_server.py"], cwd=ROOT,
                                  env=env2, stdout=fh, stderr=subprocess.STDOUT)
        try:
            got = ""
            deadline = time.time() + 1.0
            while time.time() < deadline:
                with open(logf) as f:
                    got = f.read()
                if "token:" in got:
                    break
                time.sleep(0.05)
            ok2 = "token:" in got
            check("D2：启动 1s 内日志已含 token:", ok2, f"len={len(got)}")
            if not ok2:
                print("     日志实际内容:", repr(got[:300]))
        finally:
            p2.send_signal(signal.SIGTERM)
            try:
                p2.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p2.kill()
                p2.wait()
            shutil.rmtree(dl2, ignore_errors=True)

        shutil.rmtree(dl, ignore_errors=True)

    print("\n结果: %d 项失败" % len(fails) if fails else "\n全部通过")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())

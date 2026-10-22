#!/usr/bin/env python3
"""P2-B 连接预算：/api/add 的 concurrency 不得突破 CONN_BUDGET。

评审原文：MAX_ACTIVE=3 的注释写「避免把带宽切碎」，但 /api/add 允许
concurrency 到 64 → 3×64 = 192 条连接，正好是最碎的用法。

这里只验面板层的两道闸：CONC_MAX 单任务上限 + 全局预算。
运行：python3 tests/e2e_budget.py
"""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = 8894
BASE = f"http://127.0.0.1:{PORT}"
TOK = "testtok"
HOST = f"127.0.0.1:{PORT}"
fails = []


def check(name, cond, extra=""):
    print(("  PASS " if cond else "  FAIL ") + name + (("  " + extra) if extra else ""))
    if not cond:
        fails.append(name)


def req(path, *, method="GET", body=None):
    h = {"Host": HOST, "X-Token": TOK, "Content-Type": "application/json"}
    r = urllib.request.Request(BASE + path, method=method, data=body, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=8) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def main():
    dl = tempfile.mkdtemp(prefix="fluxdl_budget_")
    env = dict(os.environ, FLUXDL_TOKEN=TOK, FLUXDL_PORT=str(PORT),
               FLUXDL_DIR=dl, PYTHONUNBUFFERED="1")
    p = subprocess.Popen([sys.executable, "fluxdl_server.py"], cwd=ROOT, env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        for _ in range(60):
            try:
                req("/")
                break
            except Exception:
                time.sleep(0.15)

        st, b = req("/api/meta")
        meta = json.loads(b)
        conc_max = meta.get("conc_max")
        budget = meta.get("budget")
        print(f"  面板元信息: conc_max={conc_max} budget={budget} max_active={meta.get('max_active')}")
        check("conc_max 已下调到合理值（≤16）", isinstance(conc_max, int) and conc_max <= 16, str(conc_max))
        check("预算存在且 ≤64", isinstance(budget, int) and 0 < budget <= 64, str(budget))

        print("== 单任务并发超上限 ==")
        st, b = req("/api/add", method="POST",
                    body=json.dumps({"url": "http://127.0.0.1:1/a.bin",
                                     "concurrency": 999}).encode())
        check("concurrency=999 被拒或夹紧（不静默接受）",
              st in (200, 400), f"status={st} body={b[:120]!r}")
        if st == 200:
            tid = json.loads(b)["id"]
            st2, b2 = req("/api/list")
            t = [x for x in json.loads(b2)["tasks"] if x["id"] == tid][0]
            got = t.get("concurrency")
            check("实际并发被夹到 conc_max", got is not None and got <= conc_max,
                  f"concurrency={got}")

        print("== 多任务总连接不超预算 ==")
        ids = []
        for i in range(6):
            st, b = req("/api/add", method="POST",
                        body=json.dumps({"url": f"http://127.0.0.1:1/m{i}.bin",
                                         "concurrency": conc_max}).encode())
            if st == 200:
                ids.append(json.loads(b)["id"])
        time.sleep(1.5)
        st, b = req("/api/list")
        tasks = json.loads(b)["tasks"]
        running = [t for t in tasks if t["state"] == "下载中"]
        total_conn = sum((t.get("concurrency") or 4) for t in running)
        check("活跃任务数 ≤ max_active", len(running) <= meta["max_active"],
              f"running={len(running)}")
        check("活跃任务总连接 ≤ 预算", total_conn <= budget,
              f"total={total_conn} budget={budget} "
              f"detail={[(t['id'], t.get('concurrency')) for t in running]}")
    finally:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
        try:
            out, _ = p.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            p.kill()
            out, _ = p.communicate()
        print("---- 服务端尾部输出 ----")
        print((out or "").strip()[-500:])
        shutil.rmtree(dl, ignore_errors=True)

    print("\n结果: %d 项失败" % len(fails) if fails else "\n全部通过")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())

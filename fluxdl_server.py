#!/usr/bin/env python3
"""
FluxDL 面板服务 — M2
单文件、零依赖（标准库 http.server），浅色极简 UI，SSE 实时进度。

安全（重要）：下载器天然是 RCE 级攻击面（它会把 URL 内容写进磁盘），所以：
  1. 只监听 127.0.0.1
  2. 所有 /api/* 都要 token（启动时生成，写进控制台与页面）
  3. 只接受 http/https 链接；每任务独立进程级线程池
"""
from __future__ import annotations

import json
import os
import secrets
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import fluxdl

HOST = "127.0.0.1"
PORT = int(os.environ.get("FLUXDL_PORT", "8730"))
TOKEN = os.environ.get("FLUXDL_TOKEN") or secrets.token_urlsafe(16)
DOWNLOAD_DIR = os.environ.get("FLUXDL_DIR") or os.path.expanduser("~/Downloads/FluxDL")


class Task:
    _seq = 0

    def __init__(self, url, *, referer=None, cookie=None, name=None, concurrency=0):
        Task._seq += 1
        self.id = Task._seq
        self.url, self.referer, self.cookie, self.name = url, referer, cookie, name
        self.requested = concurrency
        self.state = "排队"          # 排队/下载中/完成/失败/已取消
        self.dl: fluxdl.Downloader | None = None
        self.snapshot = {"percent": 0.0, "speed": 0, "eta": None, "done": 0,
                         "total": 0, "concurrency": 0}
        self.message = ""
        self.out_path = ""
        self.created = time.time()
        self._lock = threading.Lock()

    def start(self):
        self.state = "下载中"
        t = threading.Thread(target=self._run, daemon=True)
        t.start()
        return t

    def _run(self):
        dl = fluxdl.Downloader(
            self.url, DOWNLOAD_DIR, concurrency=self.requested,
            referer=self.referer, cookie=self.cookie, override_name=self.name,
            on_progress=self._on_prog)
        self.dl = dl
        try:
            res = dl.run()
            self.out_path = res["path"]
            self.state = "完成"
            self.snapshot.update(percent=100.0, speed=0, eta=0)
        except fluxdl.DownloadError as e:
            self.state = "失败"
            self.message = str(e)
        except Exception as e:                       # 兜底，别让线程静默死掉
            self.state = "失败"
            self.message = f"{type(e).__name__}: {e}"

    def _on_prog(self, p):
        with self._lock:
            self.snapshot.update(
                percent=round(p.get("percent", 0), 2),
                speed=p.get("speed", 0), eta=p.get("eta"),
                done=p.get("done", 0), total=p.get("total", 0),
                concurrency=p.get("concurrency", 0))

    def cancel(self):
        if self.dl:
            self.dl.stop()
        if self.state in ("排队", "下载中"):
            self.state = "已取消"

    def to_dict(self):
        with self._lock:
            snap = dict(self.snapshot)
        return {"id": self.id, "url": self.url, "state": self.state,
                "name": os.path.basename(self.out_path) or self.name or "",
                "path": self.out_path, "message": self.message,
                "concurrency": self.requested, **snap}


TASKS: dict[int, Task] = {}
TASKS_LOCK = threading.Lock()
MAX_ACTIVE = 3          # 同时下载任务数，避免把带宽切碎


def _pick_queued():
    with TASKS_LOCK:
        active = sum(1 for t in TASKS.values() if t.state == "下载中")
        if active >= MAX_ACTIVE:
            return
        for t in TASKS.values():
            if t.state == "排队":
                t.start()
                return


def add_task(url, **kw) -> Task:
    t = Task(url, **kw)
    with TASKS_LOCK:
        TASKS[t.id] = t
    _pick_queued()
    return t


PAGE = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FluxDL</title>
<style>
:root{--bg:#fbfbfa;--card:#fff;--line:#e8e8e5;--fg:#1f2328;--dim:#8a8f98;
      --accent:#2f6feb;--ok:#1a7f37;--err:#c9252d;--soft:#f2f4f7}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
     font:14px/1.6 -apple-system,"Segoe UI","Noto Sans CJK SC",sans-serif}
.wrap{max-width:860px;margin:0 auto;padding:40px 20px 80px}
h1{font-size:20px;font-weight:600;letter-spacing:.3px;margin:0 0 4px}
.sub{color:var(--dim);font-size:13px;margin-bottom:28px}
.row{display:flex;gap:10px;flex-wrap:wrap}
input[type=text],select{border:1px solid var(--line);background:var(--card);
     border-radius:8px;padding:10px 12px;font:inherit;color:inherit;outline:none}
input[type=text]{flex:1;min-width:240px}
input[type=text]:focus,select:focus{border-color:var(--accent)}
button{border:1px solid var(--line);background:var(--card);border-radius:8px;
     padding:10px 16px;font:inherit;cursor:pointer;transition:.15s}
button:hover{border-color:var(--accent);color:var(--accent)}
button.primary{background:var(--accent);border-color:var(--accent);color:#fff}
button.primary:hover{opacity:.9;color:#fff}
.meta{margin-top:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.meta input{font-size:13px;padding:7px 10px}
.tasks{margin-top:32px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;
      padding:14px 16px;margin-bottom:10px}
.card .top{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.name{font-weight:500;word-break:break-all}
.url{color:var(--dim);font-size:12px;word-break:break-all;margin-top:2px}
.badge{font-size:12px;padding:2px 9px;border-radius:999px;border:1px solid var(--line);
       white-space:nowrap;color:var(--dim);background:var(--soft)}
.badge.run{color:var(--accent);border-color:#cfe0ff;background:#eef4ff}
.badge.done{color:var(--ok);border-color:#c6e8d0;background:#effaf2}
.badge.err{color:var(--err);border-color:#f5cdd0;background:#fdf1f2}
.bar{height:4px;background:var(--soft);border-radius:99px;margin:12px 0 8px;overflow:hidden}
.bar>i{display:block;height:100%;background:var(--accent);width:0;transition:width .25s}
.stats{display:flex;gap:16px;font-size:12px;color:var(--dim);flex-wrap:wrap}
.stats b{color:var(--fg);font-weight:500}
.mini{display:flex;gap:8px}
.mini button{padding:4px 10px;font-size:12px}
.empty{color:var(--dim);text-align:center;padding:40px 0;font-size:13px}
.msg{color:var(--err);font-size:12px;margin-top:6px;word-break:break-all}
.solidsub{font-size:12px;color:var(--dim);margin-top:14px}
</style></head><body>
<div class="wrap">
  <h1>FluxDL</h1>
  <div class="sub">自研轻量下载内核 · 分段并发 / 断点续传 / HLS 分片合并</div>

  <div class="row">
    <input id="url" type="text" placeholder="粘贴链接（http/https/m3u8）" autofocus>
    <select id="conc">
      <option value="0">并发：自适应</option>
      <option value="1">1</option><option value="2">2</option>
      <option value="4">4</option><option value="8">8</option><option value="16">16</option>
    </select>
    <button class="primary" id="add">添加</button>
  </div>
  <div class="meta">
    <input id="referer" type="text" placeholder="Referer（可选，防盗链必填）" style="flex:1">
    <input id="cookie" type="text" placeholder="Cookie（可选）" style="flex:1">
  </div>
  <div class="solidsub" id="dirline"></div>

  <div class="tasks" id="tasks"><div class="empty">暂无任务</div></div>
</div>
<script>
const TOKEN = "__TOKEN__";
const $ = s => document.querySelector(s);
document.getElementById('dirline').textContent = "保存到 " + "__DIR__";

function fmt(n){n=+(n||0);const u=['B','KB','MB','GB','TB'];let i=0;
  while(n>=1024&&i<u.length-1){n/=1024;i++}return n.toFixed(1)+u[i]}
function eta(s){if(s==null||!isFinite(s))return '--';s=Math.max(0,Math.round(s));
  return s<60?s+'s':Math.floor(s/60)+'m'+(s%60)+'s'}
function badge(st){const m={'下载中':'run','完成':'done','失败':'err'};
  return '<span class="badge '+(m[st]||'')+'">'+st+'</span>'}

async function add(){
  const url = $('#url').value.trim();
  if(!url) return;
  $('#url').value='';
  await fetch('/api/add',{method:'POST',headers:{'X-Token':TOKEN,
    'Content-Type':'application/json'},body:JSON.stringify({
      url, referer:$('#referer').value.trim()||null,
      cookie:$('#cookie').value.trim()||null,
      concurrency:+$('#conc').value})});
}

function render(list){
  const box = $('#tasks');
  if(!list.length){box.innerHTML='<div class="empty">暂无任务</div>';return}
  box.innerHTML = list.map(t=>{
    const pct = Math.min(t.percent||0,100);
    const live = t.state==='下载中';
    return `<div class="card">
      <div class="top">
        <div style="min-width:0">
          <div class="name">${esc(t.name||t.url.split('/').pop())}</div>
          <div class="url">${esc(t.url)}</div>
        </div>
        <div class="mini">${badge(t.state)}
          ${(live||t.state==='排队')?`<button onclick="cancel(${t.id})">取消</button>`:''}
        </div>
      </div>
      <div class="bar"><i style="width:${pct}%"></i></div>
      <div class="stats">
        <span><b>${pct.toFixed(1)}%</b></span>
        <span>${fmt(t.done)} / ${t.total?fmt(t.total):'未知'}</span>
        <span>${fmt(t.speed)}/s</span>
        <span>剩 ${eta(t.eta)}</span>
        <span>连接 ${t.concurrency||1}</span>
      </div>
      ${t.message?`<div class="msg">${esc(t.message)}</div>`:''}
    </div>`}).join('');
}
function esc(s){return String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}

async function cancel(id){await fetch('/api/cancel?id='+id,{method:'POST',
  headers:{'X-Token':TOKEN}})}

$('#add').onclick = add;
$('#url').addEventListener('keydown',e=>{if(e.key==='Enter')add()});

function tick(){
  fetch('/api/list',{headers:{'X-Token':TOKEN}})
    .then(r=>r.json()).then(d=>render(d.tasks)).catch(()=>{});
}
tick(); setInterval(tick, 800);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    server_version = "FluxDL"

    def log_message(self, fmt, *args):      # 静音，避免刷屏
        pass

    def _auth_ok(self):
        if self.headers.get("X-Token") == TOKEN:
            return True
        # 页面本身允许匿名取（否则打不开），但只给静态 HTML
        return False

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, text, code=200):
        body = text.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path in ("/", "/index.html"):
            html = PAGE.replace("__TOKEN__", TOKEN).replace("__DIR__", DOWNLOAD_DIR)
            return self._html(html)
        if not self._auth_ok():
            return self._json({"error": "需要 token"}, 403)
        if u.path == "/api/list":
            with TASKS_LOCK:
                ts = sorted(TASKS.values(), key=lambda t: -t.id)
            return self._json({"tasks": [t.to_dict() for t in ts]})
        if u.path == "/api/events":           # SSE 实时推送
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            try:
                while True:
                    with TASKS_LOCK:
                        ts = [t.to_dict() for t in sorted(TASKS.values(), key=lambda x: -x.id)]
                    self.wfile.write(f"data: {json.dumps({'tasks': ts}, ensure_ascii=False)}\n\n"
                                     .encode())
                    self.wfile.flush()
                    time.sleep(0.7)
            except Exception:
                return
        return self._json({"error": "not found"}, 404)

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        if not self._auth_ok():
            return self._json({"error": "需要 token"}, 403)
        if u.path == "/api/cancel":
            q = urllib.parse.parse_qs(u.query)
            tid = int(q.get("id", ["0"])[0])
            with TASKS_LOCK:
                t = TASKS.get(tid)
            if t:
                t.cancel()
            return self._json({"ok": True})
        if u.path == "/api/add":
            n = int(self.headers.get("Content-Length") or 0)
            try:
                data = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._json({"error": "非法 JSON"}, 400)
            url = (data.get("url") or "").strip()
            if not url.lower().startswith(("http://", "https://")):
                return self._json({"error": "只支持 http/https"}, 400)
            t = add_task(url, referer=data.get("referer") or None,
                         cookie=data.get("cookie") or None,
                         name=data.get("name") or None,
                         concurrency=int(data.get("concurrency") or 0))
            return self._json({"ok": True, "id": t.id})
        return self._json({"error": "not found"}, 404)


def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"FluxDL 面板: http://{HOST}:{PORT}/")
    print(f"token: {TOKEN}")
    print(f"目录: {DOWNLOAD_DIR}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
        srv.shutdown()


if __name__ == "__main__":
    main()

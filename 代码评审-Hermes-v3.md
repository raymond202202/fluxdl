# FluxDL 复审报告 v3（Hermes）

评审时间：2026-10-22
评审对象：`baf3cad`、`023c375` 之后的工作区状态（`fluxdl.py` / `fluxdl_server.py` / `fluxdl_cli.py`）
评审方式：**全部结论来自实测**（本地 stdlib Range 服务器 + 限速，脱离外网可复现），非纯读码推断
测试床：`rangesrv.py`（stdlib-only，支持 `Range`、`Accept-Ranges`、限速、请求日志）

---

## 0. 一句话结论

续传内核和取消语义这轮**真的修好了，数据无损**；剩下的是三个"外围"问题：一个安全洞（`/` 白送 token）、一个可扩展性设计（SSE 全量快照）、一个可用性缺口（重启后丢失可续传任务）。

---

## 1. 本轮实测：v2 提的问题确认已修（带数据）

### 1.1 续传：位图 + 空洞补齐 + sha256 一致（v2 未提，本轮验证通过）

测试条件：200MB 随机源，256KB 分片，`-c 4`，限速 500KB/s，三次运行

| 轮次 | 行为 | `/tmp/rs_hits.log` 观察 |
|---|---|---|
| 第 1 次 | 跑 4s 后中断 | 32 个 Range 请求，`done: [[0, 7340032]]` |
| 第 2 次 | 跑 4s 后中断 | 请求数 32，**最小起点 7.3MB**（不是从 0 起） |
| 第 3 次 | 跑完 | 打印「续传自 14.0MB」，只请求 14.7MB→199.8MB 缺口 |

`sha256sum` 输出两行完全一致 → **续传拼接无损**。

结论：`.fluxdl` sidecar 的 `done` 区间、`_find_holes()` 补洞、`os.replace(part, path)` 成品提升这条链路是对的。

### 1.2 取消竞态：v2 P1-2 已修复（实测复核）

```
取消瞬间占用=2MB
+1s 占用=3MB  "state": "已取消"     ← 在途的 1 个分片写完
+2s … +8s 占用=3MB "state": "已取消"  ← 之后完全静止，未被"完成"覆盖
meta: {"v":4,"url":"…","size":200000000,"done":[[0,2097152]]}
残留: big200.bin  big200.bin.fluxdl  big200.bin.lock
```

- `POST /api/cancel` → `state` 变「已取消」耗时 **20ms**
- 8 秒观察窗内尺寸不再增长、状态未被 worker 改写
- 断点元数据完好，取消后重跑可续

结论：v2 P1-2「点了取消，文件继续下完，最后报完成」**已消除**。

### 1.3 元数据原子性（原本担心硬杀写坏，实测没问题）

`fluxdl.py:238-243` 与 `707-710` 都是 `tmp 文件 + os.replace`：

```python
tmp = f"{path}.tmp{os.getpid()}.{threading.get_ident()}"
json.dump(obj, f); os.replace(tmp, path)
```

SIGTERM 实测：进程立即退出、日志无收尾行，但 `.fluxdl` 完整可解析 → **数据安全，只是体验粗糙**（见 P3）。

### 1.4 v2 其他条目的状态

| v2 编号 | 内容 | 本轮结论 |
|---|---|---|
| P1-1 | 槽位泄漏 | 已修（`Downloader(...)` 已在 `try` 内） |
| P1-2 | 取消竞态 | **已修（见 1.2 实测）** |
| P2-3 | int 参数崩 500 | 已修（`_int_param` 越界/非数字回 400） |
| P2-5 | SSE token 走 query 触发 403 死循环 | 已修，且注释写清了「SSE 分支必须放在通用鉴权之前」的原因 |
| P2-8 | 前端 `innerHTML` 全量重建 | 已修（keyed ROWS + 逐行更新，进度条 transition 保留） |
| P1-3 | `probe()` HEAD 回退全读 | 本轮未复核 |
| P1-4 | 无 `--expect-sha256` 仍全量算 sha256 | 本轮未复核 |
| P1-5 | HLS 忽略 `#EXT-X-KEY`/`#EXT-X-MAP` | 本轮未复核 |

---

## 2. P1-A【安全】`/` 未鉴权内联 token，鉴权形同虚设（已实测）

### 实测

```bash
$ curl -s http://127.0.0.1:8807/            # 没有带任何鉴权头
<!doctype html> … 
$ curl -s http://127.0.0.1:8807/ | grep -o 'TOKEN *= *"[^"]*"'
TOKEN = "testtok"                            # ← token 明文出现在页面里
```

### 根因

`do_GET` 里 `/` 与 `/index.html` 分支放在通用 `_auth_ok()` 校验**之前**，且直接把 token 注入模板：

```python
PAGE.replace("__TOKEN__", TOKEN)
```

### 影响

token 唯一存在的意义，是防止**浏览器里的其他站点**（CSRF / DNS rebinding）打本机 127.0.0.1 的 API。而现在：

1. 本机任意进程 `curl 127.0.0.1:PORT/` 就能白拿 token → 进而调 `/api/add` 让服务端往磁盘任意位置写任意 URL 的文件；
2. DNS rebinding 场景下，攻击者页面把域名解析到 127.0.0.1，`fetch('/')` 能读到响应体 → 拿到 token → 保护归零；
3. 全站没有 `Host` 头校验（rebinding 的标准挡法）。

### 修法（推荐两条都做）

```python
ALLOWED_HOSTS = {f"127.0.0.1:{PORT}", f"localhost:{PORT}", f"[::1]:{PORT}"}
def _host_ok(self):
    return self.headers.get("Host", "") in ALLOWED_HOSTS   # 不一致直接 403
```

```python
# 页面不再内联 token：
# 首次打开让用户粘贴一次存 localStorage（后端仍校验 X-Token），
# 或者干脆 / 只返回静态页 + 一个 /api/session 端点（校验 Host 后下发一次性 nonce）
```

只做第 1 条能挡住 rebinding；第 2 条才是根治。

---

## 3. P1-B【性能】SSE 是「全量快照 × 全客户端」，复杂度 O(客户端 × 任务)

### 实测

200 个任务时，单次推送报文 **66,211 B**，推送频率 1.43 Hz → **92.5 KB/s / 单客户端**。

### 根因（`fluxdl_server.py` 约 528-545）

```python
while True:
    with TASKS_LOCK:
        payload = {"tasks": [t.to_dict() for t in sorted(TASKS.values(), …)]}
    self.wfile.write(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode())
    self.wfile.flush()
    time.sleep(0.7)
```

前端已经做了 keyed diff（P2-8），但**服务端一点没省**：每 0.7s 在锁内把全部任务重新序列化一遍，`url`/`path`/`concurrency` 这些永不变化的字段也每帧重发。

开 3 个标签页 = 3 份 66KB/0.7s 的序列化 + 3 倍带宽 + 3 份 GIL 争用。

### 修法（按收益排序）

1. **增量协议**：`{"v": 全局版本号, "changed": [变化任务], "removed": [id]}`，前端 `applyList` 合并；版本号没变就只写一行 `: ping`（注释行，EventSource 会忽略）；
2. **抑制静止任务**：状态没变的任务不进 `changed`；历史任务（`HISTORY_KEEP=200`）压根不参与逐帧下发，只在新增/删除时推；
3. **字段分帧**：不变字段只在任务首次出现时带，之后只发 delta。

改完 200 任务稳态推送应能压到几百字节/次。

---

## 4. P1-C【可用性】任务零持久化，磁盘上却留「表观大小=完整大小」的半成品

### 实测

```
du -h --apparent-size big200.bin  →  191M      # stat 报 200000000
du -h               big200.bin  →  7.5M       # 实占
cat big200.bin.fluxdl → {"v":4,"size":200000000,"done":[[0,7340032]]}
```

- 服务重启后 `TASKS` 是空的 → 面板一片空白；
- 但磁盘上留着一堆 `200MB 的文件` + `.fluxdl`，用户从文件管理器看**完全分不清**哪个下完了、哪个是残件；
- 而这些 sidecar 里 `url`/`size`/`done` 齐全，**恢复所需信息 100% 都在磁盘上**。

### 修法（成本最低、体感提升最大）

启动时扫 `DOWNLOAD_DIR/*.fluxdl`，读出 `url`/`size`/`done`，重建为 `state="可续传"` 的任务入队：

```python
for meta in glob(os.path.join(DOWNLOAD_DIR, "*.fluxdl")):
    m = json.load(open(meta))
    register_resumable(url=m["url"], size=m["size"], done=m["done"])
```

再加一个「继续」按钮（对 `已取消`/`中断`/`可续传` 的任务），比让用户重填 URL 强得多。

---

## 5. P2 组

### P2-A 新建任务 `name`/`path` 是空串（已实测）

```json
{"id":1,"url":"http://…/big200.bin","state":"下载中","name":"","path":"",…}
```

`to_dict` 用 `os.path.basename(self.out_path)`，而 `out_path` 在探测出文件名之前为空 → 卡片标题空白，用户看不出在下什么。

修法：`name` 回退 `os.path.basename(urlparse(self.url).path)`（一行）。

### P2-B 并发预算是乘法，注释与实现自相矛盾

`MAX_ACTIVE = 3` 注释写「避免把带宽切碎」，但 `/api/add` 允许 `concurrency` 到 64 → 3×64 = **192 条连接**，正好是最碎的用法。

修法：改全局连接预算（如总 32），每任务 `concurrency` 只在预算内分配；或把上限从 64 降到 16。

### P2-C SSE 无客户端上限、无 socket 超时

`daemon_threads` 下慢客户端阻塞在 `wfile.write` 不会拖住进程退出（这点没问题），但每个 SSE 客户端常驻一个线程 + 每 0.7s 一次全量序列化，且断连检测滞后一整个推送周期。

修法：`self.connection.settimeout(10)` + SSE 客户端数上限（如 4，超限 503）。

---

## 6. P3 退出无收尾

只捕 `KeyboardInterrupt`，systemd/docker 发的是 SIGTERM → 直接死，日志连 "bye" 都没有。

数据安全（元数据原子），但应按统一 handler 处理 SIGTERM/SIGINT：置停止标志、`stop()` 所有 task、打印「已停止 N 个任务，断点已保存，重跑同命令续传」再退出。

---

## 7. 建议修复顺序

1. **P1-A 安全**（Host 白名单 → 摘掉 HTML 里的 token）—— 唯一能造成实际损害的
2. **P1-C 启动扫 `.fluxdl` 重建可续传任务** —— 用户体感提升最大，代码量最小
3. **P1-B SSE 增量协议** —— 前两条做完再做
4. **P2-A/B/C + P3** —— 打包一起改

---

## 附：本轮复现脚本（均在本机 scratch 目录）

| 脚本 | 用途 | 结果 |
|---|---|---|
| `rangesrv.py <port> <file> <B/s>` | stdlib-only Range 服务器（限速 + 请求日志 `/tmp/rs_hits.log`） | 测试床 |
| `resume_test5.sh` | 三次运行验证续传 + 位图 + sha256 | exit 0，sha256 一致 |
| `api_probe.sh` | `/` 泄露 token / 取消延迟 / SIGTERM 行为 | exit 0 |
| `cancel_race.sh` | 取消后 8 秒状态与尺寸是否被改写 | exit 0，已修复 |
| `srv_launch.py <port>` | 前台起服（runpy + 默认信号处理，便于测信号） | 已用 |

注：`bash &` 启动的子进程会继承 `SIG_IGN` 的 SIGINT，测信号必须用 runpy 显式装回 `default_int_handler`，否则会把"信号没送到"误判成"死锁"。

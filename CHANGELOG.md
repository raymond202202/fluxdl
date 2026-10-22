# Changelog

本项目的所有重要变更都记录在此。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.4.0] - 2026-10-22

外部代码复审第三轮（Hermes v3）的 P1/P2/P3 全部修复。本版解决三个"外围"问题：
一个安全洞、一个可扩展性设计、一个可用性缺口。

### 安全

- **`/` 不再内联 token**（P1-A）。原先 `do_GET` 把静态页分支放在鉴权之前，
  且直接把 token 注入模板返回：本机任意进程 `curl 127.0.0.1:PORT/` 就能白拿凭证，
  DNS rebinding 场景下攻击页面也能读到响应体 → 鉴权形同虚设。现在页面是纯静态的，
  也不再内联下载目录。
- **Host 头白名单**（P1-A）。所有请求（含静态页）先校验
  `Host ∈ {127.0.0.1:PORT, localhost:PORT, [::1]:PORT}`，不一致直接 403，
  堵住 DNS rebinding。
- **SSE 凭证改为一次性 nonce**（P1-A）。`EventSource` 不能自定义请求头，
  凭证只能走 query，而 URL 会进浏览器历史/代理日志。现在 `POST /api/nonce`
  （需先有合法 `X-Token`）下发 10 分钟有效、用后即焚的 nonce；
  旧的 `?token=` 不再被接受。

### 新增

- **重启后自动重建可续传任务**（P1-C）。启动时扫 `DOWNLOAD_DIR/*.fluxdl`，
  校验 `url` / `size` / `done` 后重建为「可续传」任务，面板给出「继续」按钮。
  此前重启后磁盘上明明有残件，用户却得重填 URL。
- 新增 `POST /api/resume?id=`，对「可续传/已取消/中断/失败」的任务重新入队，
  按 sidecar 从缺口继续，按剩余量重算并发。
- `tests/e2e_security.py` / `e2e_resume.py` / `e2e_budget.py`：三个端到端自测脚本，
  自带 stdlib Range 服务器（支持限速、可模拟"不实现 HEAD"），全程本机无外网依赖。

### 性能

- **SSE 改为增量协议**（P1-B）。原先每 0.7s 在锁内把全部任务 `to_dict()`
  再全量 `json.dumps`：200 个任务时 66,211 B/次（92.5 KB/s/客户端），
  且 O(客户端 × 任务数) 的 GIL 争用。现在只推 `{v, changed, removed}`，
  变化字段做 delta，无变化只发一行 `: ping`；新连接首帧带 `full`。
- **SSE 客户端上限 + socket 超时**（P2-C）。最多 4 个订阅者（超出 503），
  `settimeout(10)` 让"拔网线"在 write 时就报错，不再靠下一个推送周期才发现。
- **连接预算**（P2-B）。原先只卡 `MAX_ACTIVE=3` 却允许单任务 64 并发 →
  最多 192 条 TCP，与"避免把带宽切碎"的注释直接矛盾。现在
  `CONC_MAX=16` + 全局 `CONN_BUDGET=32`，`_pick_queued()` 按连接数而非任务数放行。

### 修复

- **SIGTERM 收尾是假的**（P3-2，本轮实测发现）。`_shutdown()` 调完
  `t.cancel()`（只置位就返回）立刻 `raise SystemExit`，daemon 下载线程被一起带走，
  内核 `_flush_meta()` 一行没执行 → `.fluxdl` 零字节，重启扫到 0 个可续传任务；
  而日志照样打印"断点已保存"。现在 cancel 后 join 下载线程（上限 8s），
  并**按 sidecar 是否真的落盘**输出三档日志，不再谎报。
- **`probe` 不再信任 HEAD 的错误页**（P1-3，本轮实测发现，比复审描述更严重）。
  HEAD 返回 4xx/5xx 时 `pool.open()` **不抛异常**，主分支于是把错误页的
  `Content-Length`（实测 482）当成文件大小、把缺失的 `Accept-Ranges` 当成"不支持"，
  使"HEAD 回退 GET"的分支成为死代码 → 完全支持 Range 的服务器被判成不可续传 →
  静默走 `_run_single()`（无 sidecar），中断即断点全丢。现在 HEAD 必须 2xx 才可信，
  否则回退 GET；回退分支认 `206` 或 `Accept-Ranges: bytes` 任一；
  `size` 只从 200 全量响应取。
- **新建任务 `name`/`path` 为空的空卡片**（P2-A）。`out_path` 在探测出文件名前是空串，
  卡片标题就空白。现在按 `out_path → 面板指定名 → URL basename` 依次回退。
- **SIGTERM/SIGINT 统一收尾**（P3）。原先只捕 `KeyboardInterrupt`，
  systemd/docker 发的 SIGTERM 直接被带走，日志连 bye 都没有。

### 变更

- `Task` 新增 `delta(prev)`（SSE 增量用）与 `thread` 字段（关机 join 用）。
- 回归测试 16 → 18 项（新增 `TestProbeNoHead` 两条，把 Bug 6 固化为断言）。

## [0.3.0] - 2026-10-22

外部代码复审第二轮遗留的 P2 级问题全量修复。本版不含架构改动，重点是
"消除净亏损"与"把静默失败变成可诊断的失败"。

### 新增

- CLI `-v/--verbose`：文件系统不支持空洞检测（tmpfs、NFS/CIFS、FUSE）时，
  提示检测能力已降级为抽样，避免误以为做了全量检查。
- 面板 `/api/events`（SSE）真正被前端订阅。原先服务端实现完好却无人订阅，
  前端一直跑 800ms 轮询。现在有变化即时推送；`EventSource` 不可用或长连接
  被中继掐断时自动退回轮询。
- 回归测试从 10 个扩到 16 个，覆盖本轮全部可单测的修复点。

### 修复

- **小文件不再做自适应测速**：`< 8 MiB` 直接固定 4 并发。此前无论多小都要
  先跑一遍 1.5s / 3MB 的测速，2 MiB 文件测速比下载本身还慢。
- **面板外部输入统一校验**：`id`、`concurrency` 等参数解析失败或越界时返回
  `400`，此前直接抛 `ValueError` 变成 `500` + 整段 traceback 进响应。
- **任务列表不再无限增长**：终态任务保留最近 200 条，活跃任务永不淘汰。
  此前跑一整天后每次列表请求都要全量序列化所有历史任务。
- **token 比较改为常量时间**：`hmac.compare_digest`，堵计时侧信道。
- **短读报错带上诊断**：输出未完成区间、应收/实收字节、短读次数，
  替代原来那句零信息量的"仍有区间未完成"。
- 修正完成态快照：小文件下载快于进度回调的节流窗口时，`done/total` 停在 0，
  面板显示"0 B / 未知"而进度条 100%。现用实际下载字节补齐。

### 变更

- `verify_file()` 增加 `verbose` 参数（默认关闭，不影响现有调用）。

## [0.2.0] - 2026-10-22

外部代码复审第二轮的两个 P0 与五个 P1。

### 修复

- **截断响应不再被当成成品**：单连接路径比对 `Content-Length` 与实际字节数，
  不符则删除残件并报错。真实场景：CDN 边缘、对象存储签名 URL、反向代理超时、
  服务端限速掐连接。
- **去重键不再退化为文件名**：改为 `(host + path, 产物名)`，只在查询串不同时
  视为同一目标。此前 `v1/app.zip` 与 `v2/app.zip` 会被判为同一文件，
  第二个请求静默返回"完成"而文件从未下载。
- 任务槽位泄漏：`Downloader` 构造移进 `try`，保证 `finally` 一定归还名额。
- 取消竞态：`cancelled_before_start` 标志，避免"点了取消却下完还报完成"。
- 探测路径不再把整个文件读进内存（`drain()` 加 64KB 上限，`probe()` 只读头）。
- 无 `--expect-sha256` 时跳过全量摘要计算，避免把加速优势吃掉一半。
- HLS 拒绝 `#EXT-X-KEY(METHOD!=NONE)` 与 `#EXT-X-MAP`，不再拼出"成功"的坏视频。

### 新增

- `LICENSE`（MIT）、`pyproject.toml`（显式声明零第三方依赖）、
  `scripts/check_stdlib_only.py`、GitHub Actions CI（3.10/3.12/3.13）。

## [0.1.0] - 2026-10-22

首个可运行版本。

### 新增

- 多线程 Range 下载内核，自适应并发。
- Range 不可用时退化为单连接。
- 断点续传（`.part` + 区间 meta，重启后按已完成区间继续）。
- 内容校验：空洞检测（`SEEK_DATA`/`SEEK_HOLE`）、全零块抽样、gzip 魔数、可选摘要比对。
- HLS（m3u8）下载 + ffmpeg 合并。
- 浅色极简 Web 面板：任务队列、进度、取消、内容校验。
- 零第三方依赖（仅 HLS 合并需要外部 `ffmpeg`）。

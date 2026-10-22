# FluxDL — 自研轻量下载器

调研 + 实现一体的项目。四份文档 + 三个源文件。

## 文件

| 文件 | 内容 |
|---|---|
| `调研报告.md` | 市面上主流下载器的完整梳理：IDM/FDM/XDM/aria2/Motrix/qBittorrent/Transmission/yt-dlp/you-get/lux/Seal 的原理拆解、实测数据、横向对比、结论 |
| `FLARE问题记录.md` | 本项目过程中 **Flare 运行环境** 的真实失败/错误/中断，含根因与优化建议（第 11 条「迭代上限」已修复并回写） |
| `BUG修复记录.md` | 两轮 bug 复盘：①probe 变量遮蔽、中断丢字节、空洞文件被误判完成；②2026-10-22 第二轮（并发静默失效、面板零校验、队列卡死、锁竞态、空洞漏检） |
| `代码评审-Hermes.md` | Hermes 对本项目的只读逐行评审（含 `文件:行号` 与实测证据） |
| `修复单-给flare-2026-10-22.md` | Hermes 出具的修复单 + 后端接口设计（阶段 0/1/2） |
| `技术选型实战分析-2026-10-22.md` | 是否换 Go/Rust 的实测结论：不换，先修算法 bug 值 13.7× |
| `ABC兼得方案-2026-10-22.md` | 自研内核 + 外挂后端的组合方案 |
| `家庭带宽实测-2026-10-22.md` | 真实网络环境实测：瓶颈是宽带出口，不是代码 |
| `基准测试资产-给flare-2026-10-22.md` | 基准测试台资产说明与复现方法 |
| `fluxdl.py` | 自研下载内核（标准库，零依赖） |
| `fluxdl_server.py` | 浅色极简 Web 面板（标准库 http.server + SSE） |

## 快速开始

```bash
# 探测一个链接
python3 fluxdl.py --probe https://example.com/file.zip

# 下载（并发自适应）
python3 fluxdl.py https://example.com/file.zip -o ~/Downloads

# 下载并强校验（推荐脚本/CI 使用，不匹配退出码 2）
python3 fluxdl.py URL -o dir --expect-sha256 <官方sha256>

# 带防盗链
python3 fluxdl.py URL --referer https://site.com --cookie "k=v"

# 起面板
python3 fluxdl_server.py       # → http://127.0.0.1:8730/
```

## 已实现的能力

- **probe**：HEAD 探测 `Accept-Ranges` / `Content-Length` / 文件名 / 是否 m3u8；HEAD 被拒时用单字节 GET 读 `Content-Range`
- **302 跟随**：保持 Range 头跨域重定向（修过 npmmirror 探测出 size=88 的 bug）
- **keep-alive 连接池**：每线程复用 TLS 连接（否则每次 Range 重握手，python.org 单次首包 12.7s）
- **自适应并发**：先测 1.5s 单连接吞吐 → ≥8MB/s 用 1、≥2MB/s 用 4、否则用 8
- **断点续传**：区间表 + 原子写 + 相邻合并，中断重跑只补缺口
- **内容校验**（新增）：下载后自动检测全零空洞 / gzip 魔数；可选 `--expect-sha256` 比对，不符退出码 2
- **并发保护**（新增）：同一目标文件加进程锁，两个 fluxdl 抢同一文件时明确报错，不再静默互相覆盖
- **无 Range 降级**：单连接顺序下载，且**禁止谎报续传**
- **HLS**：master playlist 选最高码率 → 分片并发 → ffmpeg concat → AAC 修正
- **面板**：仅监听 127.0.0.1 + token + 协议白名单，SSE 实时进度，最多 3 并行

## 验证结果

| 项 | 结果 |
|---|---|
| 续传后 sha512 vs 官方 integrity | **完全一致** |
| CLI 与 Web 双通道 sha256 | 均为 `ef67f8d8...` |
| HLS 64 分片 | 465.6MB / 635.9s / h264+aac 可播 |
| 面板无 token / `file://` | 403 / 400 正确拒绝 |
| **中断→续传→sha256 vs PyPI 官方** | `ea9bd1a8...` **完全一致** |
| **空洞/损坏文件识别** | 9/9 项自测通过 |
| **并发修复前后（200MiB @8MB/s/连接）** | 25.7s → **1.8s（14.2×）**，`-c 1` 仍 25.7s |
| **面板队列推进** | 5 任务（上限 3）全部完成，无卡死 |
| **面板内容校验** | 坏文件报「校验未通过」，不再谎报完成 |

## 已知坑（血泪教训）

- **文件大小正确 ≠ 下载完成**。`truncate(total)` 预分配让文件瞬间"大小达标"，
  里面可能全是 0x00。验收必须校验内容（gzip/tar/sha256），不能只看 `ls -l`。
- **中断时已写入的字节必须记账**，否则续传会重下、meta 与磁盘内容不一致。
- 这两个问题叠加会产生"**静默产出损坏文件还报成功**"，是最危险的失败模式。

## 没做（有意为之）

- 站点提取器（几百个站点的解析维护）→ 直接用 yt-dlp
- BT/磁力 → 是另一套协议原语，直接用 qBittorrent
- Electron 壳 → 300MB 内存换一个 GUI，不值

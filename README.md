# FluxDL — 自研轻量下载器

调研 + 实现一体的项目。**多线程下载内核 + Web 面板**，零第三方依赖。

> **零依赖**：只用 Python 标准库，不需要 `pip install`，可离线运行。
> 唯一的外部程序依赖是 HLS 合并所需的 `ffmpeg`（按需检测，缺失时明确报错）。

## 安装与运行

```bash
# 直接跑（无需安装任何东西）
python3 fluxdl.py --probe https://example.com/file.zip
python3 fluxdl.py https://example.com/file.zip -o ~/Downloads

# 或安装为命令
pip install .
fluxdl https://example.com/file.zip -o ~/Downloads

# Web 面板
python3 fluxdl_server.py       # → http://127.0.0.1:8730/
```

## 文件

| 文件 | 内容 |
|---|---|
| `fluxdl.py` | 自研下载内核（标准库，零依赖） |
| `fluxdl_server.py` | 浅色极简 Web 面板（标准库 http.server + SSE） |
| `fluxdl_cli.py` | 命令行入口封装 |
| `tests/test_regression.py` | 回归测试：把每个 P0 事故固化成断言 |
| `调研报告.md` | 市面上主流下载器的完整梳理：IDM/FDM/XDM/aria2/Motrix/qBittorrent/Transmission/yt-dlp/you-get/lux/Seal 的原理拆解、实测数据、横向对比、结论 |
| `BUG修复记录.md` | **三轮** bug 复盘：①probe 变量遮蔽、中断丢字节、空洞误判；②并发静默失效、面板零校验、队列卡死；③截断被当成品、按文件名去重 |
| `FLARE问题记录.md` | 开发环境（Flare）自身的问题档案：超时丢输出、后台进程不分离、迭代上限强制停止等 |
| `代码评审-Hermes.md` / `代码评审-Hermes-v2.md` | 外部逐行评审（含 `文件:行号` 与实测复现） |
| `性能优化记录.md` | **跨文档汇总**：优化前后对照总表、13.7×/14.2× 两次测量差异说明、算法 vs 语言（11 倍差）、已做/待做/不做路线图 |
| `修复单-给flare-2026-10-22.md` | Hermes 出具的修复单 + 后端接口设计（阶段 0/1/2） |
| `技术选型实战分析-2026-10-22.md` | 是否换 Go/Rust 的实测结论：不换，先修算法 bug 值 13.7× |
| `ABC兼得方案-2026-10-22.md` | 自研内核 + 外挂后端的组合方案 |
| `家庭带宽实测-2026-10-22.md` | 真实网络环境实测：瓶颈是宽带出口，不是代码 |
| `基准测试资产-给flare-2026-10-22.md` | 基准测试台资产说明与复现方法 |

## 用法

```bash
# 探测一个链接（不下载）
python3 fluxdl.py --probe https://example.com/file.zip

# 下载（并发自适应）
python3 fluxdl.py https://example.com/file.zip -o ~/Downloads

# 下载并强校验（推荐脚本/CI 使用，不匹配退出码 2）
python3 fluxdl.py URL -o dir --expect-sha256 <官方sha256>

# 带鉴权头
python3 fluxdl.py URL --referer https://site.com --cookie "k=v"

# 诊断模式：文件系统不支持空洞检测（tmpfs/网络盘）时会提示降级
python3 fluxdl.py URL -v

# HLS：不复用已下载分片，全部重下（服务端不给 ETag/Last-Modified、
# 又怀疑源被换过时的唯一可靠手段，见下「已知限制」）
python3 fluxdl.py https://example.com/index.m3u8 -o dir --no-reuse
```

## 测试

```bash
python3 -m unittest discover -s tests -v      # 44 个用例
python3 scripts/check_stdlib_only.py          # 零依赖校验
```

CI 在 Python 3.10 / 3.12 / 3.13 上运行同一套测试（见 `.github/workflows/tests.yml`）。

## 实测效果

| 项目 | 结果 |
|---|---|
| **大文件加速**（python.org 40MB，限速 12.6Mbps） | 25.7s → **1.8s（14.2×）**，`-c 1` 仍 25.7s |
| **面板队列推进** | 5 任务（上限 3）全部完成，无卡死 |
| **面板内容校验** | 坏文件报「校验未通过」，不再谎报完成 |
| **小文件** | < 8MiB 跳过自适应测速，省掉 1.5s 空转 |
| **面板实时性** | SSE 推送（0.7s），无 SSE 时自动退回 800ms 轮询 |

## 已知坑（血泪教训）

- **文件大小正确 ≠ 下载完成**。`truncate(total)` 预分配让文件瞬间"大小达标"，
  里面可能全是 0x00。验收必须校验内容，不能只看 `ls -l`。
- **EOF 也不等于收全**。服务器声明的 `Content-Length` 与实际收到的字节数必须比对 ——
  无 Range 的服务器在抖动/限速掐连接时会提前关闭，残件会被当成成品。
- **文件名不是身份**。按文件名去重会让 `v1/app.zip` 与 `v2/app.zip` 互相顶掉，
  第二个文件从未下载却显示"完成"。
- **中断时已写入的字节必须记账**，否则续传会重下、meta 与磁盘内容不一致。
- **"有指纹"要按物理事实算**。只回 `Content-Length` 的服务端看起来给了信息，
  但它发现不了"同长度的内容替换"；当成有身份处理，就等于把警告和校验一起关掉。
- 这些问题叠加会产生"**静默产出损坏文件还报成功**"，是最危险的失败模式：
  撞锁至少会红，静默成功不会。

## 已知限制：HLS 分片复用

HLS 的分片是分开下的，中途失败/被取消后重跑会复用 `.xxx.segments/` 里的旧片。
复用判据分三层，**从强到弱**：

| 层级 | 依据 | 能发现什么 | 代价 |
|---|---|---|---|
| 1. 清单结构 | 清单 URL、分片数、`sha1(分片URL)` | 换源/换清晰度/重新切片 | 0（清单本来就要拉） |
| 2. 服务端身份 | 每片 `ETag` / `Last-Modified` | 同 URL 同长度但**远端内容被换** | 每片一次 HEAD（无 Range 服务端走 1 字节 GET） |
| 3. 本地分片哈希 | 清单里记的 `sha1`（**下载完成后**才算） | 本地文件被截断 / 手工替换 / 旧版本残留 | 0（纯读盘） |

**限制（物理上做不到，别指望它）**：服务端既不给 `ETag` 也不给 `Last-Modified`，
同时远端把分片换成了**长度完全相同**的别的内容，而本地那片又没被动过 ——
此时"复用的旧片 == 远端现在的片"这件事在本地无从判断，本工具会**按清单复用**。

这是刻意的取舍：把默认行为改成"没指纹就全重下"会让 0 请求续传退化成全量下载，
等于废掉续传（实测 4 片小文件 0.1s vs 重下耗时随片数线性增长；大文件更是灾难）。
所以选择**明确警告而不是静默**：

```
警告: 服务端未提供 ETag/Last-Modified，无法确认分片未变，已按清单复用
      （本地完整性已校验；如需强制重下请加 --no-reuse）
```

一次运行只警告一次（不是每片一次）。看到这行、又确实怀疑源被换过时，
用 **`--no-reuse`** 强制全部重下（等价于 API 里的 `reuse=False`）。

> 注意：**长度不算内容身份**。很多简易静态服务 / 部分 CDN 会回 `Content-Length`
> 但不回 `ETag`，如果把长度当身份，`l:6768 == l:6768` 会让人误以为"已经确认没变"，
> 警告也永远不会出现 —— 这正是本轮修掉的坑。

## 没做（有意为之）

- 站点提取器（几百个站点的解析维护）→ 直接用 yt-dlp
- BT/磁力 → 是另一套协议原语，直接用 qBittorrent
- 加密 HLS（`#EXT-X-KEY`）/ fMP4（`#EXT-X-MAP`）→ **明确拒绝**而不是假装成功
- Electron 壳 → 300MB 内存换一个 GUI，不值

## 许可

MIT，见 [LICENSE](LICENSE)。

## 关于本仓库

本文档集记录一个下载器从调研、选型到修复的完整过程，**所有实测数据均来自本地限速测试环境**。

已做脱敏处理：
- 个人路径统一为 `~/`
- 局域网 IP 与 WiFi BSSID 已替换为示例值
- 不含任何账号、密码、API Key 或私钥
- 测试用 token 均为一次性随机生成或明显的假值（如 `testtok`）

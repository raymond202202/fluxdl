# FluxDL 复审报告 v4（Hermes）

对象：`/home/fantastic/Projects/downloader-research`，HEAD = `7eab94d`（`125e58d` 之后的一轮）
基线：`scripts/check_stdlib_only.py` → 通过（fluxdl.py / fluxdl_server.py / fluxdl_cli.py，零第三方 import）；
`python3 -m unittest discover -s tests -q` → **Ran 18 tests, OK (2.168s)**；工作区干净。
方法：只做黑盒实测（本地 ThreadingHTTPServer 模拟源站，可控 Range / 换内容 / 慢速 / 无 Range），
不以自述和注释为依据；每条结论都有脚本与原始输出（见附录）。

---

## 0. 一句话结论

v3 的四条 P0/P1（SIGTERM 死锁、`/` 内联 token、SSE 全量快照、面板 800ms 轮询）**确实修好了，我用数字复核过**。
但风险点换位置了：现在的问题全在**「磁盘上的断点/分片」与「服务端此刻的内容」是否还对得上**——
服务端换了同名同大小的文件，fluxdl 仍按偏移续传（E1/E4）；
HLS 残留分片按序号直接复用，源换了照样合并（E5/E7b，**零手工、纯自然路径**）；
不支持 Range 的源两进程同下，失败的覆盖成功的，而先完成那个已经打印了「完成」（E3a）。
这三处都能在 `rc=0` + 「校验通过」的情况下产出错误文件。
另有一条判据错误：`verify_file()` 对**合法的全零文件必然误判为损坏**（E2/E2b）。

| 编号 | 级别 | 问题 | 状态 |
|---|---|---|---|
| P0-1 | P0 | 续传不绑定内容（无 ETag/If-Range），换文件静默混合 | 新 |
| P0-2 | P0 | HLS 分片按序号复用、无指纹，换源后混剪（成功后自毁证据） | 新 |
| P1-1 | P1 | 无 Range 源并发同写：报完成但成品被失败进程覆盖 + 裸 Traceback | 新 |
| P1-2 | P1 | `verify_file()` 对合法全零文件必然误判 | 新 |
| P2-B | P2 | CLI 侧 SIGTERM 无 handler，丢最后一次检查点（实测丢 512 KiB / 25%） | 新 |
| P2-C | P2 | 面板 `/api/add` 丢弃 `sha256`，与 CLI 能力不对等 | 新 |
| P2-D | P2 | 同一 m3u8 被 GET 3 次 | 新 |
| P2-E | P2 | HLS 成功后 `rmtree` 分片目录（与 P0-2 叠加 → 证据自毁） | 新 |

---

## 1. 本轮实测：v3 提的问题确认已修（带数据）

### 1.1 P0【死锁】SIGTERM 立即退出，无挂起（E1/E4）
`rc=-15`，进程 0.1s 内消失，`a.bin.fluxdl`（87 B）+ `a.bin.lock`（7 B）留在盘上，重跑能续传。
v3 里「SIGTERM 后卡住不退、要再按一次」的现象没有再出现。

### 1.2 P1-A【安全】token 不再进页面（代码 + 实测）
`GET /` 返回纯静态页；SSE 走 `POST /api/nonce`（需 `X-Token`）→ 一次性 nonce → `/api/events?nonce=`。
`_burn_nonce` 用 `hmac.compare_digest`，用过即焚；`?token=` 长期凭证已失效（我实测 `GET /api/events?token=…` → 403）。
顺带：`Host: 127.0.0.1`（**不带端口**）也被 403 —— 严格反而正确，抗 DNS rebinding。

### 1.3 P1-B【性能】SSE 全量快照 → 增量（实测降 ≈380 倍）
v3 基线：66,211 B/帧 @1.43 Hz = **92.5 KB/s / 客户端**。
本轮（E6，1 个 4 MiB 任务以 60 KB/s 下载，读 4.2s SSE 流）：

```
头: HTTP/1.0 200 OK
4.20s 内 body=1024 B  事件帧=7  首帧=0.0s
有效速率=0.2 KiB/s  (v3 基线 92.5 KiB/s/客户端)
```

代码侧确认：`Task.delta(prev)` 只发变化字段，`sent` 逐帧比对，idle 心跳独立。

### 1.4 P2-5【面板】800ms 轮询 → EventSource（代码 649-709）
附带处理了「nonce 一次性 → 自动重连必 403」的坑（709 附近注释与重建逻辑）。

### 1.5 P1-3【带宽】HEAD 探测不再拉全量 body（E4 日志，0 次全量 200 GET）
```
HEAD /a.bin 200 size=4194304 etag="4194304-1792801459088410489-ea11…" if_range=None
GET  /a.bin 206 0-262143/4194304   …
（HEAD 1 次；206 共 4 次；**没有任何 200 全量 body**）
```

### 1.6 停机文案（E4b，实测与事实一致）
```
已停止 1 个任务，断点已保存（.fluxdl），重跑同命令/点「继续」可续传
落盘: ['probe.bin', 'probe.bin.fluxdl', 'probe.bin.lock']   ← .fluxdl 真的在
```
v3 里「0 字节也报已保存」的说法已改成计数/分歧文案（`fluxdl_server.py:1016-1020`）。

### 1.7 面板 `name` 空串（v3 P2-A）：已回退链修复（`to_dict()`）
`basename(out_path) → self.name → URL basename → url`，实测下载中/完成都非空。
`path` 在下载中仍为 `""`（`out_path` 尚不确定），完成态有值 —— 语义正确，不再是问题。

### 1.8 `--expect-sha256` 缺省时的措辞
`校验通过（未提供 --expect-sha256，已跳过摘要比对；空洞/抽样/gzip 魔数检查已完成）`
——明确说了「跳过」，v3 的「无条件说校验通过」不再成立。（但见 P0-1：这句「校验通过」在内容被换时仍然照打。）

---

## 2. P0-1【正确性】断点不绑定内容：换文件仍按偏移续传，静默混合

### 实测 E1（同大小、无 ETag 变化感知）
```
size=4194304  sha(A)=c65adf808a824690  sha(B)=5551c3f3083b76c4
第一次下载 rc=-15（75.0% 781 KB/s 时 SIGTERM）
续传 rc=0  →  续传自 2.5MB  耗时=0.2s
输出: 校验通过（未提供 --expect-sha256，已跳过摘要比对；…）
sha(成品)=d1172a24975fd1b9   ==sha(A)? False   ==sha(B)? False
```
成品 = A 的前 2.5 MiB + B 的后 1.5 MiB，**没有一行警告**。

### 实测 E4（源站提供强 ETag —— 客户端拿到了，但从不使用）
```
第一次: HEAD 200 etag="4194304-1792801459088410489-ea11…"
续传:   HEAD 200 etag="4194304-1792801459100410265-319eaf…"   ← 内容已换，ETag 变了
        4 次 GET 206 全部 if_range=None
meta 内容: {"v":4,"url":…,"size":4194304,"done":[[0,524288]]}   ← 不存 etag / last-modified
成品 sha=74bb740fabe4e848  ==A?False ==B?False 混合?True
```

### 根因
- `_load_meta/_new_meta`（`fluxdl.py:282-286`）字段只有 `v/url/size/done`；
- 续传请求只拼 `Range:`，没有 `If-Range:`（grep 全仓 0 命中）；
- 判断「能否续传」的唯一依据是 `url 相同 + size 相同`（`fluxdl.py:282`）。

### 影响
CDN 回源换版本、源站重压/重编码、`nginx autoindex` 同名覆盖、把 `.fluxdl` 连同半成品拷到另一台机器续传——
这几种都是**同 URL 同大小**，于是全部静默混合。而输出还写着「完成」「校验通过」。
这是本项目当前最危险的一条：它不会崩，只会给你一个坏文件。

### 修法（改动小，收益最大）
1. meta 增加 `etag`、`last_modified`（HEAD 已经在发，零额外请求）；
2. 续传时带 `If-Range: <etag>`（无 etag 用 `If-Unmodified-Since: <lm>`）；
3. 服务端返回 200（而非 206）或 ETag 与 meta 不符 → 明确失败：
   `源已变更（ETag … → …），断点作废；加 --force-restart 覆盖重下`；
4. 两者都没有时降级：比对 `Content-Length` + 已下区间的**首 4 KiB 抽样 sha**（`verify_file` 的抽样器可复用）。

---

## 3. P0-2【正确性】HLS 分片按序号复用、无任何指纹

### 实测 E5（预置残留分片）
```
对照(干净目录): rc=0  分片请求=6  成品 sha=b43d8d59c38c7ed9
带残留分片:     rc=0  分片请求=5  成品 sha=5f2b3b270b70c1d4   stderr=''
```

### 实测 E7 → E7b（**完全自然路径，无手工造状态**，这条最该看）
1. E7：PATH 里没有 ffmpeg → `失败: 分片已下完，但缺 ffmpeg 无法合并`，`rc=1`，
   分片保留：`['00000.ts','00001.ts','00002.ts','concat.txt']`（这个降级行为本身是对的）；
2. E7b：源站把这三个分片换成**同大小（9024 B）不同内容**的片子（蓝 → 红），
   `PATH` 正常后**重跑同一条命令**：
```
起始残留分片 sha: ['705e0bc9954c','705e0bc9954c','705e0bc9954c']
源站已换为 red.ts, sha: 2597f758ec4f  9024 B
rc=0
输出: 校验通过（未提供 --expect-sha256，已跳过摘要比对；…）
      完成: clip.mp4 (7.5KB) 并发=8 耗时=0.1s
请求日志: ['/c.m3u8','/c.m3u8','/c.m3u8']      ← **0 个 .ts 请求**
成品 sha=738cb6dc27f86f9d  == 纯 red.ts? False
结束时残留分片: 已清理                            ← 证据被 rmtree 删掉
```
也就是说：**只要第一次因为缺 ffmpeg（或中途被杀）失败过，装好 ffmpeg 再跑，拿到的可能是上一版内容**，
而且面板/CLI 都显示成功。用户视角完全无法察觉。

### 根因
- 分片名 `%05d.ts` 只由序号决定，`os.path.exists(next_path)` 就跳过（`fluxdl.py` HLS 下载循环）；
- 清单里的 `EXT-X-MEDIA-SEQUENCE`、分片 URI、`EXTINF` 全都不落盘、不比对；
- 成功后无条件 `shutil.rmtree(seg_dir)`，把唯一的物证删掉。

### 影响
直播回看/换清晰度/换源同名重下/首轮失败重跑 —— 前几秒是旧画面，后面是新画面。
对「离线缓存给别人看」的场景是数据正确性问题，不是体验问题。

### 修法
1. 分片名带内容指纹：`00000-<sha1(segment_url)[:8]>.ts`；或
2. `.clip.segments/.manifest` 存 `[{idx,url,dur}]`，开跑前与本次清单逐条比对，不一致就整目录作废；
3. `rmtree` 改为「保留 24h」或加 `--keep-segments`（与 P2-E 合并做）。

---

## 4. P1-1【竞态】无 Range 源并发同写：完成者的话不作数，还甩裸 Traceback

### 实测 E3（同一输出名 `dup.bin`，两个源内容不同）
```
(a) 源不支持 Range：
  A: rc=0  → 完成: …/dup.bin (4.0MB) 并发=1 耗时=13.8s
  B: rc=1  → FileNotFoundError: [Errno 2] No such file or directory:
             '/tmp/fluxdl_v4e3/out_norange/dup.bin.part' -> '/tmp/fluxdl_v4e3/out_norange/dup.bin'
  成品 sha=8d2076a14ca0b100  ==A?False  ==B?True      ← 失败的 B 覆盖了成功的 A

(b) 源支持 Range（对照）：
  2nd: rc=1 → 失败: 另一个 fluxdl 正在下载同一文件（锁 …/dup.bin.lock）。请等待它完成，…
```
(b) 的文案很好 —— 问题在于**这个锁只覆盖 Range 分片路径**：`_FileLock` 唯一使用点是 `fluxdl.py:489`。
不支持 Range 的单流路径（`download_single`）直接写 `.part` 再 `os.replace`，无锁。

### 影响
脚本批量、面板重复点、面板 + CLI 同时下同一个文件名 → 成品内容可能来自**失败的那个**，
而成功那个已经打印「完成 4.0MB」，上层脚本据此认为 OK。附带一条用户看不懂的 Traceback。

### 修法
1. 把 `_FileLock` 提到统一入口（`Downloader.run()` 解析出目标路径之后立刻加），与是否 Range 无关；
2. `os.replace` 用 `try/except OSError` 包住，转成 `DownloadLockError("目标被另一进程改动")`；
3. 复现脚本可留作回归用例（断言：两进程同下，必有一个 rc≠0 且成品 sha ∈ {A,B}）。

---

## 5. P1-2【判据错误】`verify_file()` 对合法的全零文件必然误判

### 实测 E2 / E2b
```
E2  8 MiB 全零（非稀疏）：size=8388608 blocks=16384 (=8388608, 满分配)  _find_holes -> []
    verify_file -> ok=False  '偏移 4190208 处为全零块（文件很可能是预分配/覆盖后未写入）'

E2  真稀疏对照：blocks=8  holes=[(4096, 8388608)]
    verify_file -> ok=False '发现 1 处空洞，最大一处 偏移 4096–8388608（8384512 字节）…'

E2b 8,392,704 B，全零占比 99.71%，blocks=16392，holes=[]
    verify_file -> ok=True   ← 只是因为 4 KiB 抽样窗口恰好落在 0.29% 的真实数据上
```

### 根因
判据是「抽到的 4 KiB 块是否全零」，而不是「这段是否真的未分配」。
`_find_holes()` 已经返回 `[]`（证明文件不稀疏），却仍然判损坏；E2b 证明结论由**抽样位置**决定。

### 影响
静音音轨、全黑片段、填充镜像（`dd if=/dev/zero`）、零填充分区表尾巴 —— 下完必报「内容校验未通过」。
用户唯一的反应是重下，而重下结果一样；同时它也无法再区分真正的「预分配未写入」（这才是要抓的）。

### 修法
1. 有 `SEEK_HOLE/SEEK_DATA` 支持就以 `_find_holes()` 为**唯一**判据（非稀疏即通过）；
2. 全零抽样块只在「该块前后 1 MiB 内有非零数据」时才告警（即夹在真实数据中的可疑洞）；
3. 提供 `--expect-sha256` 时以摘要为最终裁决，抽样降级为提示。

---

## 6. P2 组

### P2-B CLI 侧 SIGTERM 无 handler，丢最后一次检查点（实测）
`grep signal` 在 `fluxdl.py` 里只有 `KeyboardInterrupt`（1019）——即只处理 SIGINT；
SIGTERM 走默认动作直接终止，meta 靠 `0.4s` 周期落盘（`fluxdl.py:540`）。
E4 实测：进度条 25.0%（=1 MiB）时被杀，meta 里 `done=[[0,524288]]` → **丢 512 KiB**。
面板侧有 `_on_signal`（`fluxdl_server.py:1075-1084`），CLI 侧没有。
> 修法：CLI 也装 SIGTERM → `d.stop()` → 强制 `save_meta()` → 打印「已中断，重跑同命令可续传」（对齐 Ctrl-C 的体验）。

### P2-C 面板 `/api/add` 丢弃 `sha256`（实测）
```
add(带 sha256=全零) -> 200 {"ok": True, "id": 1, "reused": True}   ← 既没校验，也没存
grep -n sha256 fluxdl_server.py → 0 命中
```
CLI 有 `--expect-sha256`，面板没有入口 —— 两条路径的能力不对等，而面板恰恰是「给不熟命令行的人用」的那条。
> 修法：`data.get("sha256") or data.get("expect_sha256")` 透传给 `Downloader`，并回显在任务详情里。

### P2-D 同一个 m3u8 被 GET 3 次（E7b 请求日志）
`['/c.m3u8','/c.m3u8','/c.m3u8']` —— `_check_hls_supported()` 与 `_resolve_segments()` 各解析一遍，
master playlist 还要再进 media playlist。长清单/直播每 2 秒重取一次的场景会放大成 6 倍请求。
> 修法：解析一次，把 `(sessions, segs, total_dur)` 一并返回。

### P2-E HLS 成功即删分片目录（与 P0-2 叠加）
E7b 结束时 `.clip.segments` 被清空 —— 混剪发生后用户**无法自证**，我也无法让用户「先别删，我们看看」。
> 修法：保留 24h 或 `--keep-segments`（十几行）。

### P3 杂项（不单独立项）
- `.lock` 完成后残留：`fluxdl.py:855` 明确写了「残留 0 字节 .lock 无害」，是**有意设计**，不算 bug；
  但对「磁盘整洁」敏感的用户是噪音，可在 `__exit__` 里 `unlink`（Windows 除外）。
- HLS 缺 ffmpeg：文案准确（E7），但没提「已下分片会在下次被复用」——与 P0-2 叠加后这句提示的价值很高。

---

## 7. 建议修复顺序

1. **P0-1** meta 加 `etag`/`last_modified` + 续传 `If-Range`（半天，消灭最贵的静默损坏）
2. **P0-2** 分片指纹 / `.manifest` 比对（半天，顺手把 P2-E 的保留策略做了）
3. **P1-1** 锁提到统一入口 + `os.replace` 异常包装（1–2 小时）
4. **P1-2** `verify_file` 改为「稀疏性优先」（1–2 小时，附 E2/E2b 当回归用例）
5. **P2-B** CLI SIGTERM handler（1 小时，和 P0-1 一起改 meta 落盘）
6. **P2-C/D/E** 面板 sha256、m3u8 单次解析、分片保留

---

## 8. 本轮未覆盖（老实说明）

- > 4 GiB 大文件、断点跨 32 位偏移、`--rate-limit` 与并发组合；
- Windows（`msvcrt.locking` 分支只有代码阅读，无实测）;
- 面板多任务压测（MAX_ACTIVE=3 + HISTORY_KEEP=200 只读了代码）；
- HLS **直播**（无 `EXT-X-ENDLIST`、滑动窗口）——本轮清单都是点播、静态；
- 需要认证的源（Cookie/Referer 组合）。

---

## 附：本轮复现脚本（`/home/fantastic/.hermes/cache/scratch/`）

| 脚本 | 用途 | 关键输出 |
|---|---|---|
| `v4e1_resume.py` | 4 MiB 下到 75% 杀进程 → 换同大小内容续传 | `/tmp/v4e1.txt`（成品 sha 既非 A 也非 B） |
| `v4e2_verify.py` | 全零但非稀疏 / 真稀疏 / 全零占 99.71% | `/tmp/v4e2.txt`、`/tmp/v4e2b.txt` |
| `v4e3_dual.py` | 两进程同输出（有/无 Range 对照） | `/tmp/v4e3.txt`（norange 成品 == B，A 报完成） |
| `v4e4_etag.py` | 强 ETag 源续传，看 `if_range` | `/tmp/v4e4.txt`（4 次 206 全 None） |
| `v4e5_hls.py` | 预置残留分片 → 换清单 | `/tmp/v4e5.txt`（5 次请求 vs 6） |
| `v4e6_sse.py` | SSE 增量实测 | `/tmp/v4e6.txt`（1,024 B / 4.2 s） |
| `v4e7_noffmpeg.py` | 缺 ffmpeg 的降级行为 | `/tmp/v4e7.txt`（分片保留） |
| `v4e7b_reuse.py` | 装上 ffmpeg 重跑 → 复用旧分片 | `/tmp/v4e7b.txt`（0 个 .ts 请求，成品≠源） |
| `v4e8_path.py` | 面板下载中的 `path`/`name` | `/tmp/v4e8.txt` |
| `v4srv.py` | 可切 Range/norange、限速、可换内容的测试源站 | 各脚本复用 |

复跑示例：
```bash
python3 /home/fantastic/.hermes/cache/scratch/v4e7_noffmpeg.py   # 先跑这个
python3 /home/fantastic/.hermes/cache/scratch/v4e7b_reuse.py     # 再跑这个（P0-2 自然复现）
```

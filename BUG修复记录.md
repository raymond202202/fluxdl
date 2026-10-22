# FluxDL 内核 Bug 修复记录

> 本文记录在"继续开发"阶段对 `fluxdl.py` 做的三处**严重 bug 修复**。
> 这些 bug 是上一轮"已交付"时遗留的——表面测试通过，实际会**静默产出损坏文件**。
> 每条都有可复现步骤与修复前后对比。

---

## 修复总览

| # | Bug | 严重度 | 后果 | 状态 |
|---|---|---|---|---|
| 1 | `probe` 局部变量遮蔽 → `UnboundLocalError` | 高 | `--probe` 直接崩溃；**每次下载完成的收尾也崩溃** | ✅ 已修 |
| 2 | 中断时丢弃已下载字节（`got<take` 分支） | **致命** | 续传重复下载；meta 记 0 → 文件留全零空洞 | ✅ 已修 |
| 3 | 空洞文件被误判为"已完成" | **致命** | **静默产出损坏文件，还报成功** | ✅ 已修 |

---

## Bug 1：`probe` 被局部同名变量遮蔽

### 现象
```bash
python3 fluxdl.py --probe https://registry.npmmirror.com/...tgz
# UnboundLocalError: cannot access local variable 'probe' where it is not associated with a value
```

### 根因
`main()` 末尾有一段：
```python
probe = f"  实测单连接={_fmt(d.probe_rate)}/s" if d.probe_rate else ""
```
把 `probe` 用作**局部变量名**。Python 的编译期作用域规则是：
**只要函数体内出现过对某名字的赋值，该名字在整个函数内都是局部变量。**
于是第 665 行的 `probe(...)` 调用（本意是调用全局函数）被解析为"访问局部变量 probe"，
而此时它尚未赋值 → `UnboundLocalError`。

### 影响范围（比看上去大得多）
不只 `--probe` 崩。因为这段代码在 `main()` **收尾处**，
所以**每一次真实下载完成后**也会走到这里 → 崩溃。
即"下载成功了，但程序报错退出"。

### 修复
```diff
- probe = f"  实测单连接={_fmt(d.probe_rate)}/s" if d.probe_rate else ""
+ rate_note = f"  实测单连接={_fmt(d.probe_rate)}/s" if d.probe_rate else ""
```

### 验证
```bash
$ python3 fluxdl.py --probe https://.../typescript-5.6.3.tgz
{ "size": 4174590, "ranges": true, "filename": "typescript-5.6.3.tgz", "error": null }
```

---

## Bug 2：中断时丢弃已下载字节

### 现象
下载中途中断（Ctrl-C / 超时 / 断网）后：
- 磁盘上实际写入了 131072 字节
- 但 meta 记录 `done = []`
- `.fluxdl` 文件不存在

### 根因
`_worker` 中处理"没读满"的分支：
```python
if got < take:                                  # ← 中断/短读
    with self.lock:
        pool_ranges.insert(0, [a + got, end + 1])   # 把剩余放回队列
    return                                          # ← 但 got 字节被丢弃了！
with self.lock:
    self._pending.append((a, a + got))              # 只有读满才记账
```
`got` 字节**已经 `pwrite` 写入磁盘**，却因为没进 `_pending` 而永远不被记入 meta。
下次续传会把这整段当"没下过"重下（浪费带宽），
更糟的是若程序随后退出，meta 与磁盘内容**不一致**。

### 附带问题
`_flush_meta` 在 `_pending` 为空时直接 `return`，连 meta 文件都不创建，
导致"中断于第一个 chunk 之前"时完全没有续传依据。

### 修复
```diff
+ with self.lock:
+     if got > 0:
+         self._pending.append((a, a + got))   # 先记账，再决定后续
  if got < take:
      with self.lock:
          pool_ranges.insert(0, [a + got, end + 1])
      return
```
```diff
- if not pend:
-     return
  with self.meta_lock:
      for a, b in pend:
          _merge(self.meta, a, b)
-     _atomic_json(self.meta_path, self.meta)
+     _atomic_json(self.meta_path, self.meta)   # 始终落盘
```

### 验证（修复前后对比）
| | 修复前 | 修复后 |
|---|---|---|
| `meta.done` | `[]` | `[[0, 65536]]` |
| meta 文件 | 不存在 | 存在 |
| 已下载字节是否有记录 | ❌ 无 | ✅ 有 |

---

## Bug 3：空洞文件被误判为"已完成"（最危险）

### 现象
```bash
$ python3 fluxdl.py URL -o dir
完成: dir/pip-24.0.tar.gz  (2.0MB)  并发=8  耗时=60.6s     # ← 报"完成"
$ gzip -t dir/pip-24.0.tar.gz
损坏                                                        # ← 实际是垃圾
$ xxd -l 32 dir/pip-24.0.tar.gz
00000000: 0000 0000 0000 0000 0000 0000 0000 0000           # ← 全是 0x00
```

实测该文件 **417 万字节中非零字节数 = 0**。

### 根因（两个问题叠加）

**其一：预分配产生空洞**
```python
fh.truncate(self.total)     # 预分配 → 文件立刻"大小 = total"，但内容是空洞
```
本意是"避免边写边扩，让 pwrite 合法"，这本身没问题；
问题在于**后续没有任何机制校验这些空洞是否真被填满**。

**其二：用"文件大小"推断"下载完成"**
```python
fresh = not (os.path.exists(self.path) and os.path.getsize(self.path) == self.total)
if not gaps:
    self.finished = True          # ← meta 说齐了就直接成功，不校验内容
```
- 只要文件大小对得上，就认为可以续用 → 空洞被当成有效数据；
- `_gaps` 只信 meta，**从不检查磁盘上的字节是否真的存在**。

于是形成闭环：中断时 meta 丢失（Bug 2）→ 重跑时文件大小"正好"→ 判定无需下载 → 报成功。

### 修复：不信任"大小"，只信任"有效 meta"
```python
had_meta = os.path.exists(self.meta_path)
...
existed_same_size = os.path.exists(self.path) and os.path.getsize(self.path) == self.total
trusted_resume = had_meta and existed_same_size and bool(self.meta.get("done"))
if existed_same_size and not trusted_resume:
    print(f"警告: {self.path} 已存在且大小匹配，但缺少有效续传元数据 "
          f"→ 视为未完成，重新下载", file=sys.stderr)
fresh = not trusted_resume
```

### 验证
伪造一个"大小正确但全零、无 meta"的文件后重跑：
```
警告: /tmp/v2/pip-24.0.tar.gz 已存在且大小匹配，但缺少有效续传元数据 → 视为未完成，重新下载
[··························]   0.0% ...
```
✅ 不再把垃圾当成品，而是明确告警并重新下载。

---

## 端到端验证结果

完整走一遍「中断 → 续传 → 校验」：

```bash
# 1) 下载 5 秒后强制中断
→ 中断: 仍有区间未完成，重跑同命令可续传
   fetched = 65536, meta.done = [[0, 65536]]        ← 记录正确

# 2) 重跑同一条命令（续传）
→ 完成: pip-24.0.tar.gz  (2.0MB)  并发=8  耗时=60.6s  续传自 576.0KB

# 3) 完整性校验
$ gzip -t pip-24.0.tar.gz          → ✓ 通过
$ tar -tzf pip-24.0.tar.gz | wc -l  → 725 个条目
$ sha256sum pip-24.0.tar.gz
  ea9bd1a847e8c5774a5777bb398c19e80bcd4e2aa16a4b301b718fe6f593aba2
$ # PyPI 官方同文件 sha256：
  ea9bd1a847e8c5774a5777bb398c19e80bcd4e2aa16a4b301b718fe6f593aba2   ✅ 完全一致
```

---

## 反思：为什么上一轮"交付"没发现这些 bug

1. **只测了"顺利路径"**：一次成功下载看起来没问题，掩盖了"中断"路径的缺陷；
2. **验证只看大小和"程序说了什么"**：文件大小正确 + 程序打印"完成" → 被判定为通过，
   **没有做内容级校验**（gzip/tar/sha256）；
3. **`--probe` 与下载收尾共用 `main()` 尾段**，而 probe 崩溃被当成"probe 功能没写好"，
   没意识到它同时炸掉了下载收尾；
4. **没有"坏状态注入"测试**：如"伪造空洞文件""中断后重跑"这类，
   恰恰是下载器最容易出错的地方。

**结论：下载器的验收标准必须是"内容校验通过"，而不是"文件存在且大小对"。**
建议后续把 `sha256/sha512 校验` 做成 `fluxdl.py` 的内置选项（如 `--expect-sha256 <hex>`），
把"验证"从人工环节变成程序环节。

---

## 补充：Bug 4（测试中发现）：并发下载互相覆盖

### 现象
测试时同一个 URL 被启动两次，产出**大小正确但全零**的文件。
一开始误判为 Bug 3 未修好，实际是**两个进程同时写同一目标**。

### 根因
`truncate(total)` 预分配 + 各自 `pwrite`，两个进程都认为自己是唯一写入者，
互相覆盖对方的区间；最终谁也没写全，但文件大小是对的。

### 修复：下载前对目标路径加进程级排他锁
```python
class _FileLock:
    """对目标路径加进程级排他锁，避免两个 fluxdl 同时写同一文件。"""
    def __enter__(self):
        self.fh = open(self.path, "a+")
        fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        # 拿不到 → DownloadLockError，明确提示而非静默覆盖
```

### 验证
```
PASS | 并发下载被锁挡住 | exit=1
B 输出: 失败: 另一个 fluxdl 正在下载同一文件（锁 .../pip-24.0.tar.gz.lock）。
        请等待它完成，或确认没有残留进程后删除锁文件。
```

---

## 回归自测（9 项全过）

```
PASS | probe 可用                     | size=2132709 ranges=True
PASS | 空洞文件被识破                 | 偏移 0 处为全零空洞（文件很可能是预分配后未写入）
PASS | 正确 sha256 通过
PASS | 错误 sha256 被拒
PASS | 非 gzip 内容被识破             | gzip 魔数不符（文件已损坏）
PASS | 并发下载被锁挡住                | exit=1
PASS | --expect-sha256 存在
PASS | --no-verify 存在
PASS | 已下载文件 sha 与官方一致       | ea9bd1a847e8c577
========================================================
总计 9/9 通过
```

---

# 第二轮修复（2026-10-22，依据 Hermes 评审 + 修复单）

本目录原本**不是 git 仓库**，改前已 `git init` 并提交基线，另存 `.backup-20261022/`。
改动前锚点校验通过：`fluxdl.py` = `c359e318…`、`fluxdl_server.py` = `79cbbcd8…`，与评审一致。

## 一、修复清单与验收结果

| # | 严重度 | 问题 | 位置 | 验收 |
|---|---|---|---|---|
| P0 | 🔴 致命 | **并发完全失效且静默**：`n = min(concurrency, len(gaps))`，全新下载 `gaps` 只有 1 段 → 只起 1 个线程 | `fluxdl.py:465` | ✅ 25.7s → **1.8s** |
| P0 | 🔴 | **面板零校验**：`dl.run()` 后直接 `state="完成"`，从不调 `verify_file` | `fluxdl_server.py:59` | ✅ 坏文件报"校验未通过" |
| P0 | 🔴 | **队列永久卡死**：`_pick_queued()` 只在 `add_task` 调用一次，任务结束无人推进 | `fluxdl_server.py:52-66` | ✅ 5 任务全部完成 |
| P0 | 🔴 | **面板同 URL 撞锁**：两任务推导同名文件，后到者必失败 | `fluxdl_server.py` | ✅ 去重复用同一任务 |
| P1 | 🟠 | **锁释放时 `unlink` 造成幽灵锁竞态**：新旧 inode 上各持一把锁 | `fluxdl.py:_FileLock.__exit__` | ✅ 改为刻意不删 |
| P2 | 🟡 | **空洞检测被"一小撮数据"骗过**：只抽 3 个偏移，偏移 0 那片 2MB 空洞漏检 | `fluxdl.py:743` | ✅ 改用 `SEEK_HOLE` |
| P2 | 🟡 | **每 800ms 全量重建 DOM**：进度条 transition 失效，点"取消"可能点空 | `fluxdl_server.py:209` | ✅ 改为按 id 差分更新 |
| P3 | 🔵 | 主题配色 | `fluxdl_server.py:123` | ✅ `#2f6feb` → `#6d4aff` 淡紫 |
| P3 | 🔵 | 日志撒谎：打印的是用户入参而非实际生效值 | `fluxdl.py:465` | ✅ 打印「请求/实际/块数」 |

## 二、并发修复：真正的根因

评审给的方向是对的，但**实测发现根因比评审描述的更偏一层**：

```python
# 修复前
pool_ranges = [[a, b] for a, b in gaps]
n = max(1, min(self.concurrency, len(pool_ranges)))   # len()==1 → n=1
```

首次修改后**仍不生效**（ESTAB=2、nlwp=1、7.9MB/s），排查发现：

1. `_gaps()` 全新下载返回 `[[0, total]]`，长度恒为 1；
2. 但**光改这里还不够** —— 我第一版改完后 `log(...)` 引用了**不存在的函数 `log()`**，
   而该分支在 try 内被吞掉，导致 `n` 的计算没生效。这是"修复本身静默失败"。

**最终改法**（线程数上界由「可切出的块数」决定）：

```python
_gap_bytes = sum(b - a for a, b in pool_ranges)
_nblk = (_gap_bytes + CHUNK - 1) // CHUNK      # CHUNK = 256 KiB
n = max(1, min(self.concurrency, _nblk))
if self.verbose:
    print(f"并发: 请求={self.concurrency} 实际={n} 块数={_nblk}", file=sys.stderr)
```

## 三、实测数据（200 MiB，每连接限速 8 MB/s）

| 配置 | 耗时 | 速度 | SHA256 |
|---|---|---|---|
| `-c 1`（单连接上限） | 25.7 s | 7.8 MB/s | ✅ 一致 |
| **`-c 16`（修复后）** | **1.8 s** | **120.5 MB/s** | ✅ `ce83d46d…` 一致 |
| 小文件 97.7KB `-c 16` | 0.0 s | — | ✅ n 自动降为 1 |

**并发提升 14.2×**。`-c 1` 仍是 25.7s，证明提速**纯粹来自并行**、没有边界回退。

## 四、面板验收（P0-1/P0-2/P0-3）

| 测试 | 结果 |
|---|---|
| 5 任务（MAX_ACTIVE=3） | 1-3 同时下 → 完成后 4-5 **自动开始**，无"排队"残留 |
| 5×40MB 数据正确性 | 5/5 SHA256 与源一致 |
| 同 URL 连点 3 次 | `id=1 reused=false` → `reused=true` ×2（不再撞锁） |
| 稀疏坏文件走面板路径 | `state=失败`，`内容校验未通过: 发现 1 处空洞 偏移 0-4194304` |

## 五、续传验收

| 场景 | 结果 |
|---|---|
| 中断后**有**有效 meta（已下 14.68MB） | 续传自 14.0MB，SHA 一致 ✅ |
| 中断后**无** meta（预分配 40MB 但没来得及写 meta） | **拒绝续传并重下**，SHA 一致 ✅ |

第二条是内核最有价值的设计：**"大小对但无有效 meta"绝不信任**，宁可重下。

## 六、本次新增的两个缺口（评审未提及，实测发现）

1. **既有 verify_file 会放过"非稀疏的全零文件"**
   `truncate` 预分配后被显式写零时，`SEEK_HOLE` 看不到空洞，但内容同样是坏的。
   已在稀疏检测之外**保留抽样全零检查**（两者互补，不可只留其一）。
2. **面板去重只比 URL 字符串不够**
   实测 `?u=1` / `?u=2` 指向同一文件，按 URL 比对漏判 → 仍然撞锁。
   改为按「目标文件名」去重（优先显式 `name`，否则取 URL 末段，与内核推导一致）。

---

# 第三轮：Hermes 复审 v2（2026-10-22）

> 基线：`代码评审-Hermes-v2.md`。上一轮修 P0 时**自己引入了一条回归**，
> 且"不要只信文件存在/大小"的教训没有贯彻到底 —— 这轮的核心不是加功能，是把静默成功彻底堵死。

## 一句话回顾

上一轮结束时我以为"内容校验通过"就等于安全了。复审 v2 指出：
**文件大小对、内容校验也"通过"，但它仍然是残的。**
两个 P0 的共同特征都是「报错为零」。

## 修复总览

| # | 问题 | 严重度 | 后果 | 状态 |
|---|---|---|---|---|
| P0-1 | `_run_single()` EOF 即转正，不比对 `Content-Length` | **致命** | 声明 100000 实收 60000，仍然 `校验通过` + 退出码 0 | ✅ 已修 |
| P0-2 | `_target_key()` 退化成裸文件名 | **致命** | v1/v2 同名包、不同站同名 exe 被判同一目标，第二个**从未下载**却显示"完成" | ✅ 已修 |
| P1-1 | `Downloader(...)` 构造在 `try` 之外 | 高 | 构造异常 → 线程死、名额不归还，3 次后面板再也起不了新任务 | ✅ 已修 |
| P1-2 | 取消竞态 | 高 | 点取消后文件继续下完，最后状态被覆盖成"完成" | ✅ 已修 |
| P1-3 | `drain()` = `resp.read()` 无上限 | 高 | probe 回退路径把 1.5GB 读进内存 | ✅ 已修 |
| P1-4 | 无 `expect` 也全量算 sha256 | 中 | 200MiB 文件下载 1.8s、哈希 1–2s，自己吃掉一半加速 | ✅ 已修 |
| P1-5 | HLS 忽略 `#EXT-X-KEY` / `#EXT-X-MAP` | 中 | 密文分片/无 moov 的 fMP4，ffmpeg 返回 0 → 报"完成" | ✅ 已修 |

## P0-1：截断文件被当成成品

### 复现（已固化为测试）
socket 服务器声明 `Content-Length: 100000`、实发 60000 后优雅关闭，且**不返回
`Accept-Ranges`**，逼内核走 `_run_single()`。旧代码：

```
run() 返回成功: {'size': 60000}
verify_file: (True, 'ffb240...')
```

### 根因
```python
buf = r.read(65536)
if not buf:
    break                     # ← EOF 静默 break
os.replace(part, self.path)   # ← 直接转正
```
`verify_file()` 四道检查全部看不见：非稀疏无空洞、数据是真的 0x01、非 gzip、
无 `expect` 时 sha256 只算不比。

### 修复
`os.replace()` 之前加断言，并清掉 `.part`：
```python
if self.total and n != self.total:
    os.remove(part)
    raise DownloadError("响应被截断：Content-Length=%d，实收 %d 字节（差 %d）")
```

### 为什么这条最危险
无 Range 的服务器**很常见**：CDN 边缘节点、对象存储签名 URL、反向代理配错。
网络抖动/限速掐连接都会走到这条路径。用户拿到一个能打开但内容残缺的 zip/安装包/pdf。

## P0-2：按文件名去重 —— 上一轮修复引入的回归

### 实测
```
a.com/v1/pack.zip -> 判为同一目标: True
b.com/v2/pack.zip
```

### 根因
上一轮为修 P0-3（同 URL 连点撞 `_FileLock`）把去重键降级成裸文件名：

```python
def _target_key(url, kw):
    name = kw.get("name")
    if name: return name
    return os.path.basename(urlparse(url).path) or url
```

**文件名不是身份。** 上一轮末尾我自己写的"改为按目标文件名去重"，
恰恰就是这条 bug 的源头 —— 为了兜住 `?u=1/?u=2`，牺牲了 host 和 path。

### 修复
```python
u = urlparse(url)
name = kw.get("name") or basename(u.path) or url
return (u.netloc.lower() + u.path, name)
```
忽略 query 就满足 `?u=1/?u=2` 的原诉求，不需要牺牲 host/path。

## P1 五条

### P1-1 任务槽位泄漏
构造挪进 `try`，`finally` 里的 `_pick_queued()` 因此一定执行。
（重写 `_run()` 时踩了一次坑：把原内层 `try` 的 `except` 缩进搞错，见 FLARE问题记录。）

### P1-2 幽灵取消
三处同时改：
1. `cancel()` 在 `dl is None` 时置 `cancelled_before_start = True`；
2. `_run()` 开头检查，已取消则立刻 `return`（但仍走 `finally` 归还名额）；
3. 写 `state="完成"` 前再检查一次，取消状态不被覆盖。

### P1-3 探测/收尾无界读
`drain(resp, max_bytes=65536)`，`resp.read(max_bytes)`；probe 两处传 `0` 只读头。
进入这条分支的前提正是"服务器不标准"，所以它才是危险路径。

### P1-4 无谓的全量哈希
`if not expect: return True, ""` —— 没有比对对象时摘要纯属净亏损。

### P1-5 HLS 静默产出坏视频
新增 `_m3u8_guards()` + `_check_hls_supported()`：
- `#EXT-X-KEY:METHOD` 非 `NONE` → 明确报"加密流不支持"；
- `#EXT-X-MAP` → 明确拒绝（直接 concat 得到无 moov 的坏文件）。

另外 `subprocess.run(ffmpeg)` 补 `timeout=1800`，避免永久"合并中"且不响应取消。

## 验证：测试不是文档，测试会变红

新增 `tests/test_regression.py`，10 个用例。关键做法是**验证测试本身有效**：

```
# 临时去掉 P0-1 的断言
$ python3 -m unittest discover -s tests -k truncated
FAIL: test_truncated_single_conn_raises
AssertionError: DownloadError not raised      ← 修复回退则变红

# 恢复修复
$ python3 -m unittest discover -s tests
Ran 10 tests in 1.129s
OK
```

一个不会变红的测试等于没写。

## 工程化（复审第 6、8 条）

| 项 | 处理 |
|---|---|
| `LICENSE` | 新增 MIT —— 公开仓库无授权 = 别人连合法 fork 都算不上 |
| `pyproject.toml` | 新增；`dependencies = []` 显式声明零依赖，`[project.scripts]` 提供 `fluxdl` 命令 |
| `fluxdl_cli.py` | 新增入口封装（内核 `main(argv)` 需要显式传参，console_scripts 不带参数） |
| `.github/workflows/tests.yml` | 新增 CI：3.10/3.12/3.13 矩阵 + compile + unittest + 零依赖校验 |
| `scripts/check_stdlib_only.py` | 新增；用 `sys.stdlib_module_names` 守着"零依赖"这个卖点 |
| 事故档案入库 | `BUG修复记录.md` / `代码评审-Hermes.md` / `代码评审-Hermes-v2.md` / `FLARE问题记录.md` 全部提交 |

**为什么这些文档必须进仓库**：别人 clone 下来只看到"一个下载器"，看不到
"为什么这么写"。这几个 P0 的修复逻辑，靠读代码是读不出来的。

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

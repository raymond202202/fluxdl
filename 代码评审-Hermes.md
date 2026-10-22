# 代码评审 — Hermes 视角（2026-10-22）

> 本文档由 **Hermes** 只读评审生成，**未修改任何代码**。
> 修复工作留给 **flare**。每条问题都给了 `文件:行号`、实测证据和可直接套用的改法。
> 评审方式：全文阅读 1,210 行 + 实际执行下载验证（不是静态猜测）。

---

## 0. 版本锚点（flare 改之前先对一下，避免改错版本）

| 文件 | 行数 | SHA256 |
|---|---|---|
| `fluxdl.py` | 859 | `c359e318847e085e02c6836b335a751fdc224fcc984fc0efcdb6e3dd86eae827` |
| `fluxdl_server.py` | 351 | `79cbbcd83f00294fff71c26100b0bdf9479f9514a8cb9aadc1870eea38352d75` |

- 本目录**不是 git 仓库**（无版本历史，改前请先手工备份）
- 验证环境：Fedora 44 / 内核 7.2.11-200.fc44 / btrfs
- 文件时间戳均为 2026-10-20 22:12–22:47

---

## 1. 实测背书（这些是跑过验证的，别当成"应该没问题"）

评审时真实执行，不是看代码推断：

| 验证项 | 结果 |
|---|---|
| `--probe https://registry.npmmirror.com/typescript/-/typescript-5.6.3.tgz` | 302 正确跟到 `cdn.npmmirror.com`，`size=4174590` 准确，`ranges=true` |
| 真实下载 `pip-24.0.tar.gz` | 2,132,709 字节 / 28.3s，自适应判定 `c=8`（单连接实测 12.5KB/s → 自动提到 8 档）**逻辑生效** |
| sha256 独立复核 | `ea9bd1a847e8c5774a5777bb398c19e80bcd4e2aa16a4b301b718fe6f593aba2` — **与 PyPI 官方逐字节一致** |
| 内容完整性 | `gzip -t` 通过；`tar -tzf` 列出 725 个条目 |

**结论：内核（`fluxdl.py` 的 IO 层）是可靠的，不需要大改。问题集中在"第二个入口"和几个边界条件上。**

---

## 2. 修复清单

### 🔴 P0-1 面板完全不做内容校验 —— `BUG修复记录.md` 的教训在面板层失效

**位置**：`fluxdl_server.py:52-66`（`Task._run`）

现状：

```python
res = dl.run()                      # 59 行
self.out_path = res["path"]
self.state = "完成"                  # 61 行 ← 直接报完成，零校验
```

而 `verify_file()` 的调用点**只有一处**：`fluxdl.py:840`（CLI 的 `main()` 里）。

**问题**：同一个内核，CLI 会告诉你"文件坏了"，面板会告诉你"✓ 完成"。
`BUG修复记录.md` 结尾写的"验收标准必须是内容校验通过，而不是文件存在且大小对"，在面板路径上原封不动复活了。面板恰恰是最容易被信任的那个入口。

**修法**（`fluxdl_server.py`，`dl.run()` 之后、`state="完成"` 之前插入）：

```python
res = dl.run()
self.out_path = res["path"]
ok, info = fluxdl.verify_file(res["path"])
if not ok:
    self.state = "失败"
    self.message = f"内容校验未通过: {info}"
else:
    self.state = "完成"
    self.snapshot.update(percent=100.0, speed=0, eta=0)
```

注意 `verify_file` 已在 `fluxdl.py:727` 定义且 `fluxdl` 已被 import，直接调用即可。
如果面板将来要支持 sha256 比对，给 `Task.__init__` 加 `expect_sha256` 字段，从 `/api/add` 的 `data.get("sha256")` 传入。

---

### 🔴 P0-2 队列永久卡死（已实测复现）

**位置**：`fluxdl_server.py:52-66`（`_run` 收尾缺推进）、`fluxdl_server.py:113`

`_pick_queued()` 全文**只被调用一次**，在 `add_task()`（113 行）里。`Task._run()` 线程跑完后没有任何人推进队列。

**实测证据**（`MAX_ACTIVE=3`，提交 5 个任务）：

```
提交后：    任务1-3 下载中 / 任务4-5 排队
75 秒后：   任务1 失败  任务2 完成  任务3 失败
            任务4 排队  任务5 排队          ← 永远不会动
```

**修法**（`Task._run` 包一层 `finally`）：

```python
def _run(self):
    dl = fluxdl.Downloader(...)
    self.dl = dl
    try:
        res = dl.run()
        ...
    except fluxdl.DownloadError as e:
        self.state = "失败"; self.message = str(e)
    except Exception as e:
        self.state = "失败"; self.message = f"{type(e).__name__}: {e}"
    finally:
        _pick_queued()      # ← 补这一行；_pick_queued 定义在后面对运行时无影响
```

---

### 🔴 P0-3 面板并行同 URL → 必然撞锁失败（已实测复现）

**位置**：`fluxdl_server.py:52-58`（`Task` 不传 `name`）、`fluxdl.py` 的 `_FileLock`

**实测证据**：3 个并行任务下同一个 URL，**2 个直接失败**：

```
另一个 fluxdl 正在下载同一文件（锁 /tmp/fluxtest/pip-24.0.tar.gz.lock）。
请等待它完成，或确认没有残留进程后删除锁文件。
```

两个独立缺陷：

1. `_FileLock` 是为防**进程间**互相覆盖设计的，结果被面板自己人撞上 —— 说明面板层缺"同名文件"策略。
2. 这句报错是**写给 CLI 用的**。面板用户看到"请删除锁文件"完全不知道该干什么。

**修法**（两层都要做）：

**a) 同 URL 复用**（`add_task`，109 行附近）：提交前查 `TASKS` 里有没有同 `url` 且未结束的任务，有就直接返回它的 id，不要新建：

```python
with TASKS_LOCK:
    for t in TASKS:
        if t.url == url and t.state in ("排队", "下载中"):
            return t          # 前端拿到的 id 不变，不重复下载
```

**b) 不同 URL 同文件名自动改名**：在 `add_task` 里检测目标文件名是否已被占用（磁盘上存在、或另一任务占用），占用则加 `(1)` `(2)` 后缀，通过 `override_name=name` 传给 `Downloader`。

---

### 🟠 P1-4 收到 200 时 raise 前没清连接，连接池留下脏数据

**位置**：`fluxdl.py:334-337`

```python
conn, r, _ = self.pool.open(self.url, rng=(a, end))
if r.status == 200:
    raise DownloadError("服务端忽略 Range（返回 200），数据会错位")
if r.status != 206:
    raise DownloadError(f"HTTP {r.status}")
```

抛出时这个连接**带着整个未读 body 留在池里**。下一轮 `_conn_for` 会把它取出来复用 → 新请求读到错位数据（可能表现为莫名其妙的解析失败，或更糟：静默写入垃圾）。

**这不是理论情况**：`probe` 说有 Range、实际请求返回 200，恰好是防盗链、CDN 限流、链接过期最常见的表现，正是下载器最容易踩的坑。

**修法**：两个 `raise` 前都先清连接：

```python
if r.status == 200:
    self.pool._drop_all()        # 或 self.pool.drain(r)
    raise DownloadError("服务端忽略 Range（返回 200），数据会错位")
if r.status != 206:
    self.pool._drop_all()
    raise DownloadError(f"HTTP {r.status}")
```

---

### 🟠 P1-5 `_FileLock` 的 flock + unlink 竞态（经典误用）

**位置**：`fluxdl.py:710`（`__exit__` 里 `os.remove(self.path)`）

竞态过程：

1. 进程 A 释放锁并 `unlink` 锁文件
2. 进程 B 在 A 删除**之前**已 `open` 到旧 inode，拿到一个"幽灵锁"
3. 进程 C 此时 `open` 新建的锁文件，也成功拿到锁
4. **B 和 C 同时认为自己是唯一写入者** —— 正好绕开你要防的那个 bug

**修法**：**不要删除锁文件**。残留的 0 字节 `.lock` 无害（flock 在进程退出时自动释放，文件是否存在不影响加锁语义）。

```python
def __exit__(self, *a):
    try:
        fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)
    finally:
        self.fh.close()
    # 刻意不 unlink：删除会造成新旧 inode 上各持一把锁的竞态
```

如果一定要清理残留，改成在**加锁成功后**判断自己是不是新文件，而不是在释放时删。

---

### 🟡 P2-6 空洞检测会被"一小撮数据"骗过，漏检大片空洞

**位置**：`fluxdl.py:743-748`

```python
for off in (0, max(0, size // 2 - 4096), max(0, size - 4096)):
```

只抽 3 个偏移。**实测对比**（造 4MB 文件、2 处各 2MB 空洞、只在偏移 1000 和 2MB 处各写 100 字节）：

| 方法 | 结果 |
|---|---|
| 抽样 3 点（现有） | 偏移 0 **放过**（那 100 字节让整块非全零）、中间抓到、末尾抓到 |
| `SEEK_HOLE` | **精确查出 2 处：4096–2097152、2101248–4194304** |

即偏移 0 那一大片空洞（2MB）被漏掉了，而**偏移 0 恰恰是预分配文件最典型的空洞位置**。

**修法**（Linux/btrfs 支持 `SEEK_DATA`/`SEEK_HOLE`，非 Linux 回退到抽样）：

```python
def _find_holes(fd, size):
    """返回空洞区间列表；不支持的平台返回 None（调用方回退抽样）。"""
    if not hasattr(os, "SEEK_HOLE"):
        return None
    holes, off = [], 0
    while off < size:
        try:
            data = os.lseek(fd, off, os.SEEK_DATA)
        except OSError:
            holes.append((off, size))       # 到尾都是空洞
            break
        if data > off:
            holes.append((off, data))
        try:
            off = os.lseek(fd, data, os.SEEK_HOLE)
        except OSError:
            break
    return holes
```

在 `verify_file` 里优先用它，`None` 时再走原来的抽样逻辑。
副作用：**警告稀疏文件**（合法用途）会误报 —— 加个 `warn` 而不是直接 `return False`，或只在扩展名可校验（.gz/.tgz/.zip）时硬失败。

---

### 🟡 P2-7 SSE 是死代码，但 README 在宣传它

**位置**：`fluxdl_server.py:290`（`/api/events` 端点）、`fluxdl_server.py:246`（前端 `setInterval(tick, 800)`）

`/api/events` 这个 SSE 端点**前端从来没调用过**，前端用的是 800ms 轮询 `/api/list`。
而 `README.md` 第 45 行写着"SSE 实时进度"。

另外这个端点本身也有隐患：`while True` + `wfile.write`，靠写异常退出，每个 SSE 连接占一个线程，**断开的客户端若不抛异常线程会泄漏**。

**修法二选一**：
- 前端 `new EventSource('/api/events?token=...')` 接上，删掉轮询 —— 真·实时
- 或删掉 `/api/events` 端点，改 README 写"800ms 轮询"

推荐后者（轮询实现已经稳了，SSE 收益不大），至少让文档和实现对得上。

---

### 🟡 P2-8 每 800ms 全量重建 DOM，进度条动画等于没有

**位置**：`fluxdl_server.py:209-210`（`box.innerHTML = list.map(...)`）

后果：
- `.bar>i{transition:width .25s}`（154 行）形同虚设 —— 元素每次都是新建的，没有起始宽度，永远从当前值硬跳
- 点「取消」时元素可能正在被替换，**有概率点空**

**修法**：差分更新 —— 按 `t.id` 建一份 `id → 元素` 的映射，只改变化行的 `textContent` 和进度条 `style.width`，新增/删除行才动 DOM。

---

### 🔵 P3 杂项（不影响正确性，有空再改）

| # | 位置 | 问题 | 建议 |
|---|---|---|---|
| 9 | `fluxdl.py:379-405` `_measure_and_pick` | ①只测 1.5 秒，对高延迟线路不准（实测那个 12.5KB/s 主要是 TLS 握手开销摊薄，真实速度高一个数量级）②测到的数据 `drain` 掉不记账，之后重下 | 窗口拉到 2–3 秒；把测速读到的数据 `pwrite` 进文件并记入 meta，不浪费 |
| 10 | `fluxdl.py:499` `_run_single` | 不加 `_FileLock`，两个进程写同一 `.part` 会交错 | 与 `_run_ranged` 一致地持锁 |
| 11 | `fluxdl_server.py:30-34` `Task._seq` | `+= 1` 非原子，且 `Task(url)` 在 `TASKS_LOCK` 外实例化，并发 add 可能撞 id | 把实例化+append 一起放进 `TASKS_LOCK` |
| 12 | `fluxdl.py:535` `_parse_m3u8` | 只按 `BANDWIDTH` 选流，忽略 `#EXT-X-MEDIA:TYPE=AUDIO` 独立音轨 → `-c copy` 出来的 mp4 可能没声音。文档只验证了单流 h264+aac 样本 | 解析 `EXT-X-MEDIA` 音频组，合成时加 `-map 0:v -map 1:a`；至少先给个警告 |
| 13 | `fluxdl_server.py:123` | `--accent:#2f6feb` 蓝色。用户惯用配色是浅白紫底 `#6d4aff` | 改 `--accent:#6d4aff`，配套 `--badge.run` 的 `#cfe0ff/#eef4ff` 换成淡紫 |

---

## 3. ⛔ 不要动的地方（内核里做对的，别顺手"优化"掉）

flare 修上面那些时，以下逻辑**是正确的，改动会引入新问题**：

1. **`_load_meta` 只认有效 meta，不信"文件大小"**（`fluxdl.py:239-270`）—— 这是整个项目最有价值的一条判断。"大小对了但内容全零"是静默损坏的头号来源。
2. **`_worker` 短读分支：先记账再退货**（`~359-367`）—— 注释里"否则文件留空洞"说的就是这个 bug，顺序不能动。
3. **区间合并的边界条件**（`251-270`，`s <= out[-1][1]`）—— 用 `<=` 才合并相邻，改成 `<` 会退化成碎区间。
4. **无 Range 时明确拒绝续传**（`499-529`）—— 宁可重下，不谎报进度。
5. **meta 原子写**（tmp + fsync + `os.replace`）。
6. **`pwrite` 并发写 + `truncate` 预分配**。
7. **零依赖标准库** —— 这是刻意的设计选择，不要换成 requests/aria2。

---

## 4. 改完之后怎么复验（照着跑就行）

```bash
cd ~/Projects/downloader-research

# ① 内核没被改坏：sha256 必须和官方一致
python3 fluxdl.py https://files.pythonhosted.org/packages/source/p/pip/pip-24.0.tar.gz -o /tmp/t1
sha256sum /tmp/t1/pip-24.0.tar.gz
# 期望 ea9bd1a847e8c5774a5777bb398c19e80bcd4e2aa16a4b301b718fe6f593aba2

# ② P0-2 队列：起 server，提交 5 个任务，90 秒后不应有"排队"
FLUXDL_TOKEN=testtok FLUXDL_DIR=/tmp/fluxtest FLUXDL_PORT=8799 python3 fluxdl_server.py &
for i in 1 2 3 4 5; do
  curl -s -X POST http://127.0.0.1:8799/api/add -H "X-Token: testtok" \
    -H 'Content-Type: application/json' \
    -d '{"url":"https://files.pythonhosted.org/packages/source/p/pip/pip-24.0.tar.gz"}'; done
sleep 90
curl -s http://127.0.0.1:8799/api/list -H "X-Token: testtok"
# 期望：无"排队"残留；P0-3 修好后 5 个任务应复用同一条（而不是撞锁失败）

# ③ P0-1 面板校验：把下好的文件人为破坏成空洞，再让面板"续传"到它
#    面板必须报"内容校验未通过"，而不是"✓ 完成"

# ④ P2-6 空洞检测：造 sparse 文件直接测
python3 -c "
import os,sys; sys.path.insert(0,'.')
import fluxdl
p='/tmp/hole.bin'
open(p,'wb').truncate(4*1024*1024)
f=open(p,'r+b'); f.seek(1000); f.write(b'X'*100); f.seek(2*1024*1024); f.write(b'Y'*100); f.close()
print(fluxdl.verify_file(p))   # 期望: (False, ...空洞...)  —— 现状会放过偏移0那片
"
```

---

## 5. 本次未评审的范围

- `FLARE问题记录.md`（506 行 / 25KB）—— 记的是 flare 运行时的问题，**本文档未逐条核对**，需要单独过一遍并对照当前 flare 版本。
- `调研报告.md`（225 行）—— 只扫读，未做事实核查。
- HLS 路径没有真实的 m3u8 流可测，`_parse_m3u8` / 分片 / ffmpeg 合成这几段**仅静态评审**，P3-12 的音轨问题未经实测。

---

*评审生成时间：2026-10-22 · 评审者：Hermes（只读） · 修复者：flare*

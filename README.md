# dsh-bulk-io

DeepSeek Harness 插件，界面名「**大量小文件读写优化**」：一次调用处理几万个文件——读写走热线程池，复制走最便宜的路径，删除默认可逆。

> 名字四要素：**大量**（bulk，一次调用顶几万次）· **小文件**（逐个文件的系统调用才是瓶颈）· **读写**（`io`，含建立在读写之上的扫描/复制/移动/删除）· **优化**（线程池、rename、最便宜的原生路径）。

![DSH](https://img.shields.io/badge/DSH-%3E%3D0.2.1--rc.5%20%3C0.3.0-blue)
![tests](https://img.shields.io/badge/tests-30%20passed-green)
![pack](https://img.shields.io/badge/pack-13%20modules-brightgreen)
![version](https://img.shields.io/badge/version-0.1.0-blue)
![license](https://img.shields.io/badge/license-MIT-lightgrey)
![deps](https://img.shields.io/badge/dependencies-0-brightgreen)
![platform](https://img.shields.io/badge/platform-Windows%2011%20%2B%20NTFS-lightgrey)

## 痛点

让 agent 处理几万个小文件时，默认路径是「一个文件一次工具调用」：

- 每次调用 = 一次往返 + 一次系统调用，几万个文件就是几万个来回；
- 一次 `node_modules` 级别的复制能占住对话几十分钟，期间模型什么也做不了；
- 模型眼看变慢会自己「想办法」——开始猜路径、绕过工具、`rm -rf` 一个它没数过的目录；
- 真误删了没有回收站，也没有一行记录说明它删了什么。

## 它怎么解决

五个批量工具，加一个监听工具：

| 工具 | 做什么 |
|---|---|
| `bulk_write` | 一次写几千个文件，自动建目录，逐文件报失败而不是第一个就停 |
| `bulk_read` | 按列表或 glob 批量读，输出有字节上限，每个文件标明是否截断 |
| `bulk_scan` | 只统计不列举：文件数、总字节、扩展名分布、最大成员 |
| `bulk_transfer` | copy / move / sync，走最便宜的路径（同卷 move 是一个 rename） |
| `bulk_delete` | 默认移入 `<root>/.dsh-trash/` 带 manifest，可移回；`permanent` 拆分并行删 |
| `bulk_job` | 后台任务进度；`settle:true` 可以安静等到结束 |

三件事让它不把对话拖死：

- **一次调用处理整批**，系统调用在热线程池里做，模型只等一个结果；
- **超过预算自动转后台**：投影会超过 15 秒的工作立刻变成 job id，模型马上被放回；
- **默认可逆**：删除先改名，误删只需一次 rename 移回。

## 它不管什么

- **不做**：压缩归档、加密、同步盘语义。`.zst` 只是高延迟链路下的一种传法，不是归档格式。
- **做不到**：跨设备（`dsh go` 那类）传输——目标机器上的路径不在这台机器的文件系统里，没有可靠的低成本路径。
- **管不到**：读取权限。读跟随文件系统，和宿主的 read 工具一致；只有写、移动、删除受 DSH 文件策略约束。
- **不保证**：回收空间。trash 和 quarantine 只让路径立刻可用，字节由谁清理谁决定。

## 给谁用

- 经常处理**上万个小文件**的仓库：`node_modules`、构建产物、抓取结果、素材库；
- 要在一次回合里搬动或清理一大片目录，又不想让对话卡住；
- 删东西之前希望**还有一步可退**。

## 安装

```sh
# 从 GitHub 安装
dsh plugin --profile desktop add github:Knot-chord/dsh-bulk-io
# 锁版本
dsh plugin --profile desktop add "github:Knot-chord/dsh-bulk-io#v0.1.0"

# 开发时直接链本地目录（改完重载即生效，不产生副本）
dsh plugin --profile desktop add link:D:/path/to/dsh-bulk-io
```

前置条件：DSH `>=0.2.1-rc.5`、Node `>=20`、Windows 11 + NTFS。装完重载 profile 或重启 DSH，工具列表出现 `bulk_write` 等即可。自测：`node test/engine.test.mjs`（30 项，零依赖）。

### 装完先验四件事

| # | 做法 | 期望 |
|---|---|---|
| 1 | `bulk_write` 写 3 个文件，再 `bulk_read` 读回 | 内容一致。小批量只回一行结论，成本行要到 1,000 个文件以上才出现 |
| 2 | `bulk_scan` 扫一下这棵树 | 文件数、字节数、扩展名分布都对得上 |
| 3 | `bulk_delete` 删一个目录（不加 `permanent`） | 路径消失，`<root>/.dsh-trash/<时间>/` 里能找到它 |
| 4 | `bulk_transfer` 同卷 `move` 一个上千文件的目录 | 毫秒级完成，结果写着 `by rename`、`no file data was copied` |

后台任务的进度快照在 `%TEMP%\dsh-bulk-io\<jobId>.json`，24 小时后自动清理；不写进包目录。

## 原理 · [跳过，直接看配置 ↓](#配置)

不关心实现细节可以跳过本节，不影响使用。

### 三个平面：复制和写入不是一层

文件 I/O 不只是读和写。有两个原语平面，一个由它们拼出来的复合平面：

| 平面 | 操作 | 成本 | 有字节吗 |
|---|---|---|---|
| 命名空间 | create / rename / unlink / stat / readdir | 每个条目固定延迟 | 无 |
| 数据 | read / write | 每字节 + 每次调用开销 | 有 |
| 复合 | copy / move / sync / delete | 上面两者的和 | 视情况 |

所以：

- **`write` 是原语**，3 个原语/文件（create、write、close）；
- **`copy` 是复合**，5 个/文件（create、open、read、write、close）——它不是「更快的 write」；
- **`move`（同卷）是纯命名空间**，1 个 rename、零字节；
- **`delete` 是纯命名空间**，3 个/条目（lstat、unlink、分摊的 readdir）。

这条规则决定了优化的方向：**插件只能减少每文件的原子操作数，不能加快原子操作本身**。要更快就换操作类型（rename、归档流），而不是加线程——线程只摊薄延迟，设备饱和后不再有收益。

### 一次调用怎么拆：计划 → 派发 → 执行

发出多少原语由操作类型决定（上一节的表），这里说的是它们怎么被派发。计划只有几百个单元，不是几十万次调用，所以每次都能自校验：无重复、无父子重叠、权重和等于公布的总数，否则直接抛错。

```text
L3 计划   目标 → 深挖单条目链 → 只在有子目录处拆分 → 权重并行统计
L2 派发   单元按窗口切片，一次一个工作线程，进度按文件数回报
L1 执行   rmSync / copyFileSync / rename，都是系统调用本身
L0 平台   探测：robocopy、tar、clone、过滤器链；探测不到就回退
```

### 结果怎么读

```text
copied 2,500 files (732 KiB) in 1.80 s via threads (window 16) -> D:\out
406 KiB/s through the data plane · 12,500 primitives at 6,935/s (5/file) · 721 ms per 1,000 files
```

- `through the data plane` 才是带宽；纯命名空间的操作写 `namespace plane only`，不该拿它比 MB/s。
- `primitives at …/s` 是跨操作可比的单位：复制、删除、写入在同一台机器上按它对齐。
- `ms per 1,000 files` 是给自己估工期的口径，不依赖文件大小。

删除会把目录单独计数——删一万个空目录不会报「删了 0 个路径」：

```text
permanently deleted 2,500 files and 26 directories in 444 ms
namespace plane only, no file data moved · 7,500 primitives at 16,903/s (3/file) · 177 ms per 1,000 files
```

成本行只在大批量（1,000 个文件以上）出现，一次三五个文件的结果不占版面。

### 策略阶梯：先问最便宜的够不够

| 顺序 | 策略 | 适用 | 代价 |
|---|---|---|---|
| 1 | rename | 同卷 move | 1 个原语/条目，零字节 |
| 2 | clone | 同卷 copy 且卷支持 reflink | 元数据级；NTFS 不支持，探测后跳过 |
| 3 | robocopy | UNC 目标、sync | 一个进程，`/MT` 多线程 |
| 4 | tar \| zstd | 高延迟链路 | 一条流，压缩换带宽 |
| 5 | 线程池 | 其余情况 | 每文件 5 个原语，窗口自适应 |

选中的策略写在结果里（`via threads (window 16)`），不用猜。

**这不是 bug，是层的差别**：93,009 个文件同卷移动只要 251 ms，同样一批复制要 43 秒。移动一个 rename 就做完了，一个字节都没搬；复制要逐文件 create + read + write。

### 删除为什么不会误删

四道闸门，任何一道不过就拒绝：

1. **解析后判定**：目标在 `realpath` 之后必须仍在 root 之内，`..` 和 junction 都算；
2. **不删根本身**：root 自身、盘符根一律拒绝；
3. **计划自校验**：删除计划必须证明自己——无重复单元、无父子重叠、权重和等于公布总数；
4. **失败即报告**：每个失败的路径和原因都出现在结果里，不给一句「N failed」了事。

第 3 条是补出来的：计划曾经把一条单条目链当成一个单元，整个 93,000 文件的树只跑一个线程；也曾经把目录算成文件，报 103,627 而实际 93,009。现在两者都被显式的断言盯着。

### 长任务为什么不占住对话

一次工具调用期间整轮对话都在等它。所以工作总是先成为 job，再和 15 秒的计时器赛跑：

- 赢了 → 普通结果，模型根本看不到 job id；
- 输了 → 同一个 job 以句柄返回，模型立刻被放回，`bulk_job` 或进度文件接着报。

是否提前转后台是**投影**而不是计数：用本进程实测吞吐乘以待办数量，超过预算的 60% 就转。第一批判定用保守种子（400 × 核心数），之后用实测值自我修正。等不及也可以 `settle:true` 安静等它结束，而不是每三秒轮询一次。

## 数据

本机实测：Windows 11、NTFS、16 核、火绒实时防护开启。**换机器差异极大，以自己机器的原语成本为准。**

原语成本（单线程，1 KiB 文件）：

| 原语 | 耗时 |
|---|---|
| create + 写入 | 1.75 ms |
| stat | 0.48 ms |
| read | 0.51 ms |
| rename | 2.0 ms |
| unlink | 0.87 ms |

批量操作：

| 场景 | 结果 |
|---|---|
| 写 20,000 个文件 | 11,000 文件/s |
| 复制 4,708 文件 / 338 MiB | 3.39 s · 99.8 MiB/s · 6,944 primitives/s |
| 复制 8,000 × 1 KiB（并发 16） | 1,863 文件/s · 约 540 ms / 1,000 文件 |
| 同卷 move 93,009 文件 | **251 ms**（rename，零字节） |
| 永久删除 93,009 文件 | 2 分 46 秒（约 560 文件/s，树深且碎片多） |

**这不是插件慢**：复制停在 1,800 文件/s 左右，是因为单线程原语就是 create 1.75 ms + read 0.51 ms；并发 1→16 有 4.4 倍加速，之后设备饱和。能突破的只有换操作类型，不是换实现。

## 配置

默认零配置。需要时用环境变量，或按调用覆盖：

| 变量 | 作用 | 默认 |
|---|---|---|
| `DSH_BULK_CONCURRENCY` | 线程数 | 核心数的一半，夹在 2–16 |
| `DSH_BULK_RATE` | 每核心小文件吞吐的种子 | 400 文件/s |
| `DSH_BULK_PROMOTE_ABOVE` | 超过多少个文件直接转后台 | 由投影决定，一般不设 |
| `DSH_BULK_AUTO_BACKGROUND_MS` | 前台等待预算 | 15,000 ms |

调用级参数（每个工具都有）：`concurrency`、`background`、`dryRun`。删除另有 `permanent`、`quarantine`、`maxFiles`、`force`。

> [!WARNING]
> `permanent:true` 是真删，不进回收站。先 `bulk_scan` 看清数量，或先跑一次 `dryRun`，是更便宜的保险。

> [!IMPORTANT]
> 改完 `index.js` 要**重载插件行**（关掉再打开），热更新不覆盖服务端模块；环境变量在进程内读一次，改完需要重启 DSH。

## 兼容性

- **声明**：DSH `>=0.2.1-rc.5 <0.3.0`（`engines.dsh`），Node `>=20`，零运行时依赖。
- **实测**：`0.2.1-rc.5`（独立 `DSH_HOME` 沙盒：装入 → `--dump-config` 含 `id: bulk-io` → 启动无加载错误 → 模型真的调用 `bulk_scan`）、`0.2.2-rc.3`（日常使用）。
- 只核过类型、没实机跑过的版本不写进声明。DSH 升级后先确认这四个接口没变形：`ctx.tools.register`、`ctx.systemPrompt.section`、`ctx.sandboxPolicy.resolve`、`ctx.effect`。

版本之间确实变过东西：`ctx.workingDirectory` 是拿到"会话工作目录"的正规入口，拿不到时退回进程目录。

本插件的相对路径因此先问 `ctx.workingDirectory.get(session)`，再退 `process.cwd()`。

- **平台**：只支持 Windows 11 + NTFS。插件没有按系统写的配置——它运行时探测能力（`robocopy`、`tar`、卷是否支持 clone、哪个安全软件在过滤器链上），探测不到就回退。别的系统能跑但不是支持承诺。
- 卷格式：NTFS 不支持 reflink，所以 clone 这一级在这里永远不会被选中；换成 ReFS 才会，不需要改任何配置。

## token 预算

常驻只有一段提示词：**1,261 字符（约 350 token）**。工具结果按需出现，不进常驻上下文。

省 token 的入口：`bulk_scan` 代替列目录、`dryRun` 先看计划、`bulk_job` 的 `settle:true` 少轮询。

## 局限

- **取消在单元边界生效**：`rmSync` 不可中断，一个正在删的大目录会删完才停。
- **删除速率受树形影响**：扁平树约 2,100 文件/s，深嵌套真实沙箱约 560 文件/s，差距来自每层的 readdir，插件消不掉。
- **`maxFiles` 数的是顶层目标**，不是子文件；它的作用是拦住「一个 glob 选中整个盘」。
- **trash 不自动清理**：`.dsh-trash` 会一直占空间，何时清由人决定。
- **读取不受策略约束**：与宿主 read 工具一致，是跟随文件系统而非沙箱裁决。
- **无跨设备**：目标不在本机文件系统内时没有可靠的低成本路径，明确不做。

## 变更

### 0.1.0

首个可用版本。

- 批量 write / read / scan / transfer / delete + `bulk_job` 监听
- 五级传输策略：rename、clone、robocopy、tar|zstd、线程池
- 可逆删除（trash / quarantine）与永久删除，带计划自校验与逐条失败报告
- 按平面的成本报告：primitives/s、数据平面速率、每千文件耗时
- 投影式转后台，不按固定文件数

支持 DSH `>=0.2.1-rc.5`，实测 `0.2.2-rc.3`。

## 开发与验证

```sh
node test/engine.test.mjs      # 30 项：写入/读取/扫描/传输/删除/进度/安全
node lib/cli.mjs doctor        # 本机体检：原语成本、探测结果、策略选择
node lib/cli.mjs scan <path>   # 不经过 DSH 直接看一个树有多大
```

测试覆盖的几类约定：删除计划必须通过自校验；junction 逃逸必须被拒；删除后再写同一路径必须成功；传输结束后进度定时器必须停止。

## License

MIT。零运行时依赖，只用 Node 自带模块和 Windows 自带的 `robocopy`、`tar`。设计上参考了 `dsh-capability-policy` 的分层取舍与 `dsh-element-ref` 的失败可见性，未复用其代码。

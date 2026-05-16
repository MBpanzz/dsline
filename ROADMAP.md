# dsline 项目路线图与技术说明

**版本**：0.3.0-draft
**日期**：2026-05-15
**状态**：修订草案，待 ADR 确认

---

## 1. 项目定位

`dsline` 是一个面向 Python 数据处理和多进程通信场景的轻量级本地数据流框架。

它的核心目标是：

- 用共享内存提供高吞吐、低延迟的本地多进程通信。
- 在可验证安全的路径上提供真零拷贝。
- 用统一的 `Stream / Operator / Pipeline` 抽象连接通信和处理。
- 保持核心包轻量，无需外部服务即可运行。
- 使用 Rust 实现性能关键路径，通过 PyO3 提供 Python API。

### 1.1 要解决的问题

Python 现有方案各有短板：

| 方案 | 问题 |
|---|---|
| `multiprocessing.Queue` | pickle 开销高，吞吐低 |
| `multiprocessing.shared_memory` | 只有原始内存块，缺少通道、同步、背压 |
| `pyzmq` | 灵活但非共享内存零拷贝路径有限 |
| Ray / Dask | 能力强但重，启动和部署成本高 |
| 自研 mmap | 易出错，缺少统一 API 和生命周期管理 |

`dsline` 聚焦于：

> 单机多进程、本地数据流、低延迟通信、可组合流水线。

### 1.2 非目标

`dsline` 不计划成为：

- Ray / Dask 的分布式调度替代品。
- Kafka / Pulsar 的持久化消息系统替代品。
- 通用 RPC 框架。
- 任意 Python 对象零拷贝框架。
- exactly-once 消息系统。

---

## 2. 设计原则

1. **诚实的零拷贝**
   明确区分真零拷贝、单次拷贝和序列化路径，不做泛化宣传。

2. **生命周期安全优先于性能宣传**
   alloc/publish 必须经过 Rust 和 PyO3 双层验证后才能公开。

3. **核心轻量**
   核心包不引入 DataFusion 等重依赖；复杂能力通过 optional feature 或后续扩展实现。

4. **先单机，后分布式**
   优先打磨 Linux/macOS 上的本地共享内存体验，再扩展 socket、broker、持久化。

5. **传输和处理分离**
   Transport 负责数据搬运，Operator 负责数据变换，Pipeline 负责编排。

6. **慢路径显式命名**
   Python UDF 使用 `_py` 后缀，让用户清楚性能边界。

---

## 3. 系统架构

```text
+------------------------------------------------+
|                  Pipeline 层                   |
|  拓扑构建、运行时、生命周期、背压、metrics       |
+------------------------------------------------+
|                  Operator 层                   |
|  Rust 快路径：select / batch / expr-lite        |
|  Python 慢路径：map_py / filter_py              |
+------------------------------------------------+
|                  Transport 层                  |
|  shm://  bus://  unix://  tcp://                |
+------------------------------------------------+
|                  Channel 层                    |
|  SPSC -> MPSC -> 分片 MPMC                      |
+------------------------------------------------+
|                  Memory 层                     |
|  shared memory / mmap / ring buffer / lease     |
+------------------------------------------------+
```

核心组件：

| 组件 | 说明 |
|---|---|
| `Channel` | 低层通信原语，提供 send/recv |
| `Transport` | 把 shm、bus、socket、broker 封装成流端点 |
| `Stream` | 数据流抽象 |
| `Operator` | 流上的转换逻辑 |
| `Pipeline` | source → operator → sink 的运行拓扑 |
| `Lease` | 保护共享内存槽位生命周期的内部对象 |

---

## 4. 数据路径与零拷贝语义

`dsline` 明确区分三种路径：

| 路径 | 拷贝次数 | 条件 | 说明 |
|---|---:|---|---|
| 真零拷贝 | 0 | 数据直接在共享内存中分配，接收方获得 view | alloc/publish |
| 单次拷贝 | 1 | 用户已有 bytes / ndarray，被复制进共享内存 | send/recv |
| 序列化路径 | 2+ | Python 对象、嵌套结构、非连续内存 | pickle/msgpack/Arrow IPC 等 |

### 4.1 send/recv 路径

```python
ch.send(b"hello")
data = ch.recv()
```

特点：

- 适合 bytes、bytearray、buffer protocol 对象。
- 对已有用户内存通常需要一次拷贝进入共享内存。
- 第 0 阶段主要验证此路径。

### 4.2 alloc/publish 路径

```python
buf = ch.alloc(shape=(1000, 1000), dtype="float64")
buf[:] = compute_result()
ch.publish(buf)

view = ch.recv()
```

特点：

- 生产者直接写入共享内存。
- 消费者获得指向同一物理内存的只读 view。
- payload copy count = 0。

### 4.3 alloc/publish 安全门禁

alloc/publish 是项目最高风险路径，必须满足以下条件后才能公开 API：

1. Rust 侧 slot/lease/refcount 通过单元测试和 Miri 检查。
2. 并发状态机通过 loom 或等价模型测试。
3. PyO3 + numpy 集成路径通过多进程压力测试。
4. 强制 GC、随机持有 view、切片 view、延迟释放 view 均不能导致槽位提前复用。
5. publish 后 buffer 必须 seal，生产者不能继续修改已发布数据。
6. 槽位复用条件必须严格满足。

### 4.4 槽位复用条件

一个共享内存槽位只有在以下条件全部满足时才能复用：

- 槽位不处于 `WRITING`。
- 消息已被逻辑消费。
- 所有消费者 refcount 已归零。
- 所有 Python view / numpy view 已释放。
- 所有 Rust `SlotLease` / `BufferLease` 已 drop。
- 没有未完成的 `publish` 或 `recv` 操作。

内部模型：

```text
MmapRegion: Arc 持有共享内存区域
SlotLease: 保护单个槽位不被复用
ExportGuard: 绑定到 Python/numpy view 的 base 对象
BufferLease: alloc 阶段的独占写入权限
```

---

## 5. 共享内存 Channel 设计

### 5.1 阶段化实现

| 阶段 | Channel 能力 |
|---|---|
| 0.0.1 | 固定槽位 SPSC bytes |
| 0.1.0 | 变长消息、MPSC、背压、可选 alloc/publish |
| 0.2.0 | 分片 MPMC、BusTransport、metrics |
| 后续 | unix socket、broker、持久化 |

第 0 阶段不直接实现复杂变长 allocator，不公开 alloc/publish。

### 5.2 槽位状态机

```text
0 = FREE       可写入
1 = WRITING    写入者正在填充
2 = COMMITTED  已提交，可读取
3 = PINNED     已读出但仍被外部 view 持有
4 = CORRUPTED  校验失败或恢复失败
```

简化槽位头：

```text
┌───────────────┬───────────────┬───────────────┐
│ state          │ seq            │ payload_len   │
├───────────────┼───────────────┼───────────────┤
│ checksum       │ refcnt         │ export_refcnt │
├───────────────┼───────────────┼───────────────┤
│ writer_pid     │ writer_start   │ writer_epoch  │
├───────────────┴───────────────┴───────────────┤
│ payload                                           │
└──────────────────────────────────────────────────┘
```

### 5.3 SPSC

第 0 阶段只实现 SPSC：

- 单生产者，单消费者。
- head/tail 分离。
- 使用 Acquire/Release 内存序。
- 不做 CAS-heavy MPMC。
- 优先验证性能、正确性和 PyO3 绑定。

### 5.4 MPSC

第 1 阶段实现 MPSC：

- 多生产者 CAS 竞争 head。
- 单消费者顺序推进 tail。
- 每个写入者获得槽位后标记为 `WRITING`。
- 写入完成后以 Release 语义提交为 `COMMITTED`。

### 5.5 分片 MPMC

第 2 阶段不实现复杂 lock-free MPMC。

采用：

```text
N 个 SPSC / MPSC channel + 路由层
```

路由策略：

- round-robin
- hash-based
- broadcast

---

## 6. 崩溃恢复

### 6.1 问题

生产者在写入中被 `kill -9`，可能留下 `WRITING` 槽位。

### 6.2 方案

写入者获取槽位时记录：

- `writer_pid`
- `writer_start_time`
- `writer_epoch`
- `timestamp_ns`

消费者遇到 `WRITING` 槽位时，必须校验进程身份：

```text
process identity = pid + start_time
```

只有在以下情况才允许回收：

- PID 不存在；
- 或 PID 存在但 start_time 不匹配；
- 或 owner 明确执行 cleanup。

如果平台无法可靠判断进程身份，默认不自动回收，避免误伤仍在写入的活进程。

### 6.3 平台实现

| 平台 | 方式 |
|---|---|
| Linux | `/proc/<pid>/stat` 第 22 字段 starttime |
| macOS | `proc_pidinfo` / `kinfo_proc` |
| Windows | `OpenProcess` + `GetProcessTimes` |

Linux 解析 `/proc/<pid>/stat` 时需要注意：

> comm 字段可能包含空格，不能简单按空格 split。

### 6.4 checksum

checksum 用于检测数据损坏，不用于安全认证。

COMMITTED 槽位读取时仍需校验 checksum。

---

## 7. 背压策略

Channel 支持以下策略：

| 策略 | 行为 |
|---|---|
| `BLOCK` | 缓冲区满时阻塞，默认 |
| `RAISE` | 立即抛出 `BufferFullError` |
| `DROP_NEWEST` | 丢弃当前消息 |
| `DROP_OLDEST` | 丢弃最旧消息 |

第 0 阶段优先实现：

- `BLOCK`
- timeout
- `RAISE`

第 1 阶段补充 drop 策略。

---

## 8. Pipeline 运行时模型

### 8.1 同步 API

Python 用户优先使用同步 API：

```python
p = dsline.Pipeline()
p.from_transport("shm://input").pipe(...).sink("shm://output")
p.run()
```

内部模型：

- Rust 侧使用 tokio runtime。
- Python 同步 API 通过后台线程驱动 runtime。
- Python 侧暴露阻塞式迭代器和 `run/start/stop`。

### 8.2 Python UDF 与 GIL

Python UDF 不在 tokio reactor/core worker 上直接执行。

UDF stage 使用：

```text
Rust stream -> bounded queue -> Python worker -> bounded queue -> Rust stream
```

原则：

- 等待数据时释放 GIL。
- 调用 Python 回调时获取 GIL。
- 使用 batch size 摊薄 GIL 开销。
- 小 batch size 下吞吐可能显著下降，文档必须明确说明。

### 8.3 必测 benchmark

必须单独测：

- Rust-only pipeline。
- Rust ops + 1 个 Python UDF。
- Python UDF batch size = 1 / 16 / 64 / 256 / 1024。
- Rust/Python 边界切换次数。
- p50/p99 latency。
- throughput 降低比例。

---

## 9. Operator 系统

### 9.1 Rust 快路径

第 1 阶段提供：

- `select`
- `batch`
- `filter_expr`
- `map_expr`

其中 `filter_expr` 和 `map_expr` 使用 **expr-lite**。

### 9.2 expr-lite

第 1 阶段不引入 DataFusion。

expr-lite 支持：

- 数值字面量。
- 列引用。
- 算术：`+ - * /`
- 比较：`> >= < <= == !=`
- 逻辑：`and or not`
- 简单括号表达式。

不支持：

- SQL。
- 聚合。
- join。
- 字符串函数。
- Python UDF 嵌入表达式。
- DataFusion 表达式。
- 完整 Arrow 表达式框架。

未来可以评估：

- Arrow compute kernels。
- 独立 `dsline[datafusion]` 扩展。
- 更完整的表达式 DSL。

### 9.3 Python 慢路径

```python
source.map_py(fn, batch_size=256)
source.filter_py(fn, batch_size=256)
```

文档必须明确：

> Python UDF 是慢路径，吞吐通常显著低于 Rust 原生算子。

---

## 10. 依赖策略

### 10.1 Python 依赖

核心安装：

```bash
pip install dsline
```

目标：

- 不依赖外部服务。
- 不强依赖 numpy / pyarrow。
- bytes channel 可直接使用。

可选依赖：

```toml
[project.optional-dependencies]
numpy = ["numpy>=1.24"]
arrow = ["pyarrow>=14.0"]
all = ["numpy>=1.24", "pyarrow>=14.0"]
```

### 10.2 Rust 依赖原则

- 核心路径允许小而稳定的 Rust crate。
- 不在核心包引入 DataFusion。
- Arrow Rust 依赖需谨慎，优先作为 optional feature 评估。
- Broker、压缩、持久化均不进入核心 MVP。

---

## 11. API 设计

### 11.1 Channel API

```python
import dsline

ch = dsline.ShmChannel(
    "demo",
    capacity=1024,
    slot_size=4096,
    backpressure=dsline.Backpressure.BLOCK,
    timeout=5.0,
)

ch.send(b"hello")
data = ch.recv()

ch.close()
```

context manager：

```python
with dsline.ShmChannel("demo") as ch:
    ch.send(b"hello")
```

buffer protocol：

```python
ch.send(memoryview(data))
ch.send(numpy_array)  # 单次拷贝路径
```

alloc/publish，仅在 safety gate 通过后公开：

```python
buf = ch.alloc(shape=(1000, 1000), dtype="float64")
buf[:] = compute()
ch.publish(buf)

view = ch.recv()
```

### 11.2 Pipeline API

```python
import dsline
from dsline import ops

p = dsline.Pipeline()

source = p.from_transport("shm://sensor")

source.pipe(
    ops.filter_expr("temperature > 20"),
    ops.map_expr("temperature * 1.8 + 32"),
    ops.batch(256),
).sink("shm://output")

p.run()
```

### 11.3 Python UDF

```python
source.map_py(
    lambda batch: model.predict(batch),
    batch_size=256,
)
```

### 11.4 异常层次

```text
DslineError
├── ChannelError
│   ├── ChannelClosedError
│   ├── ChannelNotFoundError
│   ├── BufferFullError
│   ├── TimeoutError
│   └── CorruptedMessageError
├── TransportError
│   ├── TransportConnectError
│   └── UnsupportedTransportError
├── PipelineError
│   ├── PipelineBuildError
│   ├── PipelineRuntimeError
│   └── OperatorError
├── SerializationError
├── SchemaMismatchError
└── FeatureUnavailableError
```

原则：

- Rust panic 不暴露给 Python。
- UDF 异常保留 Python traceback。
- 可恢复错误和不可恢复错误分开。

### 11.5 CLI

```bash
dsline info

dsline shm list
dsline shm inspect demo
dsline shm cleanup --older-than 1h
dsline shm remove demo

dsline bench shm --message-size 4096 --count 1000000
dsline bench queue-compare
```

---

## 12. 内部协议

### 12.1 Frame

所有 transport 使用统一 frame：

```text
Frame Header + Metadata TLV + Payload
```

Header：

| 字段 | 类型 | 说明 |
|---|---|---|
| magic | u32 | 魔数 |
| version | u16 | 协议版本 |
| flags | u16 | 标志位 |
| kind | u16 | bytes / ndarray / arrow / control |
| header_len | u32 | header + metadata 长度 |
| payload_len | u64 | payload 长度 |
| seq | u64 | 序列号 |
| timestamp_ns | u64 | 写入时间 |
| schema_hash | u64 | schema 指纹 |
| checksum | u32 | payload 校验 |

### 12.2 Metadata TLV

```text
type: u16
length: u32
value: bytes
```

常见类型：

| Type | 含义 |
|---|---|
| 1 | dtype |
| 2 | shape |
| 3 | strides |
| 4 | column names |
| 5 | Arrow schema |
| 6 | encoding |
| 7 | user tags |

### 12.3 兼容性

- 0.x 允许协议破坏性调整。
- 1.0 后同主版本内保持协议兼容。
- 连接建立时做版本协商。

---

## 13. 平台支持策略

### 13.1 0.0.x 平台范围

| 平台 | 支持级别 | 说明 |
|---|---|---|
| Linux x86_64/aarch64 | 一级 | 功能和性能主平台 |
| macOS x86_64/arm64 | 二级 | 功能 smoke test |
| Windows x86_64 | 暂缓 | 不作为 0.0.1 验收目标 |

### 13.2 后续支持

| 平台 | 策略 |
|---|---|
| Linux | 优先支持，共享内存性能基准平台 |
| macOS | 功能完整，性能单独标注 |
| Windows | 0.1 开始技术 spike，0.2 视稳定性标记 experimental 或 supported |

Windows 需要独立处理：

- named file mapping。
- ACL。
- handle 生命周期。
- 进程身份校验。
- 无 `/proc` 的崩溃恢复路径。

---

## 14. 安全与资源治理

### 14.1 共享内存权限

默认策略：

- 通道名不直接等同于系统 shm 名。
- 实际名称包含随机 token。
- 默认仅当前用户可访问。
- 跨用户访问必须显式配置。

### 14.2 资源限制

必须支持：

- 最大通道容量。
- 最大消息大小。
- 最大共享内存总量。
- UDF batch size 上限。
- broker 最大连接数和 topic 数，后续阶段实现。

### 14.3 不可信输入

- frame parser 必须防御畸形长度、整数溢出、大分配攻击。
- checksum 不等于认证。
- TCP/broker 阶段默认不承诺公网安全。
- 如未来支持认证，必须作为明确 feature。

---

## 15. 可观测性

Channel 和 Pipeline 暴露：

| 指标 | 含义 |
|---|---|
| messages_in_total | 输入消息数 |
| messages_out_total | 输出消息数 |
| bytes_in_total | 输入字节数 |
| bytes_out_total | 输出字节数 |
| errors_total | 错误数 |
| dropped_total | 丢弃数 |
| queue_depth | 当前队列深度 |
| queue_capacity | 队列容量 |
| latency_p50_us | P50 延迟 |
| latency_p99_us | P99 延迟 |
| backpressure_total | 背压次数 |
| udf_calls_total | Python UDF 调用次数 |
| rust_python_boundary_total | Rust/Python 边界切换次数 |

Python API：

```python
stats = p.stats()
```

事件钩子：

```python
p.on_error(fn)
p.on_backpressure(fn)
p.on_stage_start(fn)
p.on_stage_stop(fn)
```

---

## 16. Benchmark 计划

### 16.1 对比对象

- `multiprocessing.Queue`
- `multiprocessing.Pipe`
- `multiprocessing.shared_memory` 手写方案
- `pyzmq`
- Unix Domain Socket
- Ray queue，仅作为参考

### 16.2 必测维度

消息大小：

```text
64B, 1KB, 4KB, 64KB, 1MB, 16MB
```

传输路径：

- bytes
- buffer protocol
- numpy 单次拷贝
- alloc/publish，安全门禁通过后
- Arrow，后续阶段

并发：

- 1 producer
- 2/4/8 producers
- 1 consumer
- N consumers，分片 MPMC 后

Pipeline：

- Rust-only pipeline
- Rust ops + 1 个 Python UDF
- Python UDF batch_size = 1/16/64/256/1024

背压：

- BLOCK
- RAISE
- DROP_NEWEST
- DROP_OLDEST

故障：

- producer crash
- consumer crash
- stale shared memory
- checksum mismatch

### 16.3 输出格式

```text
benchmark: shm_spsc_bytes
message_size: 4096
count: 1000000
throughput_msg_s: 2400000
throughput_gib_s: 9.15
latency_p50_us: 3.2
latency_p99_us: 18.7
copy_count: 1
platform: linux-x86_64
python: 3.12
rustc: 1.75
```

---

## 17. 测试策略

### 17.1 Rust 测试

- ring buffer 状态机。
- frame encode/decode。
- metadata TLV。
- backpressure。
- crash recovery。
- slot lease / refcount。
- unsafe 边界测试。

### 17.2 模型检查和内存检查

必须覆盖：

- Miri：Rust-only unsafe/lifetime 路径。
- loom：关键并发状态机。
- fuzzing：frame parser 和 metadata parser。

说明：

> Miri 不能完整覆盖 CPython/PyO3/numpy 集成路径，因此必须结合多进程压力测试。

### 17.3 Python 测试

- channel 创建、关闭、重复打开。
- send/recv bytes。
- buffer protocol。
- numpy 可选路径。
- UDF 异常传播。
- context manager。
- GC 触发和 view 生命周期。

### 17.4 集成和压力测试

- 双进程 SPSC。
- 多进程 MPSC。
- producer kill -9。
- consumer 提前退出。
- 随机消息大小。
- 随机生产/消费速率。
- 72 小时长稳测试，1.0 前必须完成。
- RSS、fd、handle、共享内存泄漏检测。

---

## 18. 项目结构

```text
dsline/
├── pyproject.toml
├── Cargo.toml
├── ROADMAP.md
├── crates/
│   ├── dsline-core/       # frame, error, lease, ring primitives
│   ├── dsline-shm/        # shared memory backend
│   ├── dsline-transport/  # transport trait, URL
│   ├── dsline-pipeline/   # stream, runtime, pipeline
│   ├── dsline-ops/        # expr-lite, native ops
│   ├── dsline-python/     # PyO3 binding
│   └── dsline-broker/     # future broker
├── python/
│   └── dsline/
├── tests/
│   ├── rust/
│   ├── python/
│   ├── integration/
│   └── stress/
├── benches/
├── examples/
└── docs/
```

拆分原则：

- `dsline-core` 不依赖 Python。
- `dsline-python` 只负责绑定和 Pythonic 包装。
- broker 不进入 MVP 核心路径。
- ops 与 pipeline 可独立测试。

---

## 19. 开发路线图

## 第 -1 阶段：技术 Spike 与 ADR

**里程碑**：pre-0.0.1
**周期**：3-5 个工作日

### 目标

在正式编码前解决影响代码结构的关键决策，避免第 0/1 阶段返工。

### 必须产出 ADR

#### ADR-001：共享内存后端

需要确定：

- Linux 0.0.1 使用 POSIX shm、memfd，还是 mmap-backed file。
- macOS 使用路径。
- Windows named file mapping 是否延后。
- shm 命名、权限、清理策略。

工作假设：

> 0.0.1 优先 Linux POSIX shm 或 mmap-backed file；Windows 延后到 0.1 spike。

#### ADR-002：变长消息实现

候选：

- 固定槽位 + 多槽拼接。
- metadata ring + payload arena。
- 小消息 inline，大消息 arena。

工作原则：

> 0.0.1 只做固定槽位 SPSC。0.1 前必须确定变长消息方案。

#### ADR-003：表达式 DSL

结论必须明确：

- 0.1 使用 expr-lite。
- 不引入 DataFusion。
- Arrow compute kernels 仅作为未来优化评估。

#### ADR-004：PyO3/numpy 生命周期模型

必须写清楚：

- `MmapRegion`
- `SlotLease`
- `BufferLease`
- `ExportGuard`
- Python base object
- numpy view
- 槽位复用条件
- Miri/loom/压力测试验收标准

### 验收标准

- 四个 ADR 完成。
- alloc/publish 生命周期不变量明确。
- 0.0.1 平台范围明确。
- 0.1 表达式范围明确。
- 变长消息不会阻塞第 0 阶段实现。

---

## 第 0 阶段：SPSC 原型验证

**里程碑**：0.0.1
**周期**：2-3 周

### 目标

验证 Rust → Python 共享内存通道的正确性、性能和基础生命周期模型。

### 范围

做：

- 固定槽位 SPSC。
- bytes send/recv。
- PyO3 `ShmChannel`。
- Linux 性能 benchmark。
- macOS 功能 smoke test。
- alloc/publish 内部 lifetime spike。

不做：

- 不公开 alloc/publish。
- 不做 MPSC。
- 不做 Windows release blocker。
- 不做 DataFusion。
- 不做完整 Pipeline。

### 任务

- [ ] 实现固定槽位 SPSC ring buffer。
- [ ] 实现 `send(bytes)` / `recv() -> bytes`。
- [ ] 暴露 Python `ShmChannel`。
- [ ] 支持 timeout。
- [ ] 实现基本 checksum。
- [ ] 编写双进程 1GB 传输测试。
- [ ] 对比 `multiprocessing.Queue`。
- [ ] Linux 功能和性能测试。
- [ ] macOS 功能 smoke test。
- [ ] Rust unsafe 路径通过 Miri。
- [ ] ring 状态机通过 loom 或等价测试。
- [ ] PyO3 集成路径做多进程 GC 压力测试。
- [ ] alloc/publish 只做内部 spike，不公开 API。

### 验收标准

- 两个 Python 进程可稳定传输 bytes。
- 1GB 数据校验全部通过。
- Linux 上相较 `multiprocessing.Queue` 有可复现性能优势。
- macOS 可通过基本功能测试。
- Windows 不作为 0.0.1 阻塞项。
- unsafe 代码集中、注释完整。
- alloc/publish 安全风险有明确结论。

---

## 第 1 阶段：核心 MVP

**里程碑**：0.1.0
**周期**：4-6 周

### 目标

提供可安装、可使用的共享内存 Channel 和基础 Pipeline。

### Channel

- [ ] 变长消息。
- [ ] MPSC。
- [ ] 背压策略：BLOCK / RAISE / DROP_NEWEST / DROP_OLDEST。
- [ ] crash recovery：PID + start_time + checksum。
- [ ] buffer protocol 单次拷贝路径。
- [ ] alloc/publish 仅在 safety gate 通过后公开。
- [ ] Windows shared memory 技术 spike，不作为稳定承诺。

### Pipeline

- [ ] `Pipeline` API。
- [ ] `from_transport("shm://...")`。
- [ ] `pipe(...)`。
- [ ] `sink(...)`。
- [ ] `run/start/stop`。
- [ ] 后台 tokio runtime。
- [ ] 有界队列和背压传播。

### Operators

- [ ] `select`
- [ ] `batch`
- [ ] `filter_expr`，expr-lite。
- [ ] `map_expr`，expr-lite。
- [ ] `map_py`
- [ ] `filter_py`

### 工程

- [ ] PyPI 发布。
- [ ] Quickstart。
- [ ] bytes channel 示例。
- [ ] numpy 单次拷贝示例。
- [ ] Pipeline 示例。
- [ ] benchmark 文档。
- [ ] Linux/macOS CI。
- [ ] Windows experimental CI，允许非阻塞失败或单独 job。

### 验收标准

- `pip install dsline` 可用。
- bytes 和 buffer protocol 可用。
- SPSC/MPSC 可用。
- Pipeline 可完成 source → ops → sink。
- expr-lite 不依赖 DataFusion。
- Python UDF 可用并保留 traceback。
- alloc/publish 若公开，必须通过 safety gate。

---

## 第 2 阶段：单机功能增强

**里程碑**：0.2.0
**周期**：4-6 周

### 目标

完善单机场景，增强可观测性和多消费者能力。

### 任务

- [ ] 分片 MPMC。
- [ ] `BusTransport`。
- [ ] `pipeline.stats()`。
- [ ] metrics。
- [ ] 背压回调。
- [ ] Arrow RecordBatch 可选路径评估。
- [ ] 更多 ops：window、split、merge、rate_limit。
- [ ] Windows shared memory backend 进入 experimental 或 supported 决策。
- [ ] 性能回归测试进入 CI。

### 验收标准

- 多生产者、多消费者场景可用。
- metrics 可观测。
- BusTransport 可用。
- Arrow 仍为 optional。
- Windows 支持状态明确。

---

## 第 3a 阶段：Unix Socket Transport

**里程碑**：0.3.0
**周期**：3-4 周

### 目标

支持非共享内存的本地跨进程通信。

### 任务

- [ ] `unix://` transport。
- [ ] 自动 fallback 可配置。
- [ ] Pipeline API 不因 transport 切换而变化。
- [ ] benchmark 对比 Unix Domain Socket 和 shm。

---

## 第 3b 阶段：轻量 Broker

**里程碑**：0.4.0
**周期**：4-6 周

### 目标

支持多对多通信拓扑。

### 任务

- [ ] `dsline broker start`。
- [ ] pub/sub。
- [ ] point-to-point。
- [ ] `tcp://host:port/topic`。
- [ ] 自动重连。
- [ ] broker metrics。

限制：

- 不持久化。
- 不承诺 exactly-once。
- 默认不面向公网安全场景。

---

## 第 3c 阶段：持久化与投递保证

**里程碑**：0.5.0
**周期**：6-8 周

### 目标

为可靠消息场景提供基础能力。

### 任务

- [ ] WAL。
- [ ] ack/nack。
- [ ] at-least-once。
- [ ] consumer group。
- [ ] TTL。
- [ ] dead letter topic。

明确不承诺：

- exactly-once。
- 跨数据中心一致性。
- Kafka 级持久化能力。

---

## 第 4 阶段：稳定化与 1.0

**里程碑**：1.0.0
**周期**：6-8 周

### 目标

API 冻结，生产可用。

### 任务

- [ ] API 冻结。
- [ ] Frame 协议兼容策略。
- [ ] 72 小时长稳测试。
- [ ] 内存泄漏测试。
- [ ] 安全审计。
- [ ] 完整文档。
- [ ] 与主流方案 benchmark。
- [ ] conda-forge 发布。

### 验收标准

- 长稳测试通过。
- 主要平台支持状态明确。
- 文档覆盖核心路径。
- API 遵循语义化版本。
- benchmark 公开且可复现。

---

## 20. 版本策略

```text
0.0.x  原型验证
0.1.x  核心 MVP
0.2.x  单机增强
0.3.x  Unix Socket
0.4.x  Broker
0.5.x  持久化
1.0.0  稳定版
```

兼容性：

- 0.x 允许破坏性变更，但应提供迁移说明。
- 1.0 后同主版本保持 API 和协议兼容。
- 废弃 API 至少保留两个 minor 版本。

---

## 21. 关键风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| alloc/publish 生命周期错误 | use-after-free / 数据损坏 | safety gate、lease 模型、Miri、压力测试 |
| PyO3/numpy GC 交互复杂 | 槽位提前复用 | Python base object 持有 ExportGuard |
| DataFusion 引入过早 | 编译体积和复杂度失控 | 0.1 仅 expr-lite |
| Windows 共享内存复杂 | 延期和 bug | 0.0.1 不承诺 Windows |
| PID 复用 | 错误回收槽位 | PID + start_time |
| GIL 限制 UDF 性能 | 吞吐下降 | batch、慢路径命名、benchmark |
| MPMC lock-free 复杂 | 难验证 | 分片 SPSC/MPSC |
| benchmark 不稳定 | 结论不可信 | 固定环境、输出上下文、多次运行 |
| 用户误解零拷贝 | 预期错误 | 文档明确 copy count |
| shm 泄漏 | 用户体验差 | CLI cleanup、owner 生命周期 |

---

## 22. 剩余待决策问题

必须在对应阶段前关闭：

| 问题 | 最晚关闭时间 |
|---|---|
| 共享内存后端 | 第 -1 阶段 |
| 变长消息方案 | 第 -1 阶段或 0.1 开始前 |
| expr-lite 语法范围 | 第 -1 阶段 |
| alloc/publish 生命周期模型 | 第 -1 阶段 |
| Python async API 是否进入 0.2 | 0.2 规划前 |
| Arrow 支持深度 | 0.2 规划前 |
| broker 打包方式 | 0.4 规划前 |
| 压缩是否支持 | 0.5 后评估 |

---

## 23. 文档计划

必须文档：

- Quickstart。
- Zero-copy 语义。
- Channel API。
- Pipeline 教程。
- Backpressure。
- Python UDF 性能说明。
- Benchmark 报告。
- 平台支持说明。
- 崩溃恢复说明。
- FAQ。
- Troubleshooting。
- API Reference。

示例：

| 示例 | 内容 |
|---|---|
| `bytes_channel.py` | 最小 send/recv |
| `multiprocessing_replace.py` | 替换 Queue |
| `numpy_send.py` | buffer protocol 单次拷贝 |
| `numpy_zero_copy.py` | alloc/publish，安全门禁通过后 |
| `pipeline_basic.py` | source → ops → sink |
| `python_udf.py` | UDF 慢路径 |
| `backpressure.py` | 背压策略 |
| `shm_cleanup.py` | 资源清理 |

---

## 24. 零拷贝声明模板

README 和文档中统一使用以下表述：

> dsline 在共享内存 alloc/publish 模式下，对满足条件的连续 ndarray payload 提供真零拷贝传输。
> 对已有用户内存中的 bytes、bytearray、numpy ndarray，send/recv 模式通常需要一次拷贝进入共享内存。
> 对任意 Python 对象，dsline 不承诺零拷贝，通常需要序列化。

禁止使用：

- “所有数据零拷贝”
- “任意 Python 对象零拷贝”
- “比所有队列都快”
- “exactly-once”
- “无锁 MPMC 保证稳定”

推荐使用：

- “payload copy count = 0”
- “send 已有数组时 copy count = 1”
- “Python UDF 是慢路径”
- “0.5 提供 at-least-once，不提供 exactly-once”

---

## 25. 最小可行开发顺序

建议实际开发顺序：

1. 固定槽位 SPSC bytes。
2. PyO3 `ShmChannel`。
3. Linux 双进程 benchmark。
4. Miri/loom/压力测试。
5. buffer protocol 单次拷贝。
6. 简单 Pipeline。
7. expr-lite。
8. Python UDF。
9. 变长消息。
10. MPSC。
11. alloc/publish，只有 safety gate 通过后公开。
12. metrics 和 BusTransport。
13. socket / broker / 持久化。

0.1 前不建议做：

- DataFusion。
- 完整 SQL。
- broker。
- 持久化。
- consumer group。
- exactly-once。
- 复杂 lock-free MPMC。
- Windows 稳定承诺。

---

## 26. 总结

`dsline` 的核心价值不是再造一个重量级消息队列，而是提供一个轻量、本地优先、声明式的数据通信流水线：

- Channel 解决本地多进程高性能通信。
- Transport 统一 shm、bus、socket、tcp。
- Pipeline 把通信和处理组合起来。
- Rust 承担性能关键路径。
- Python 保持简单 API。
- 零拷贝能力必须建立在可验证的生命周期安全之上。

项目推进必须坚持三条边界：

1. **先安全，再零拷贝。**
2. **先 Linux 单机打磨，再跨平台扩展。**
3. **先 expr-lite 和核心 Pipeline，再考虑 DataFusion、broker 和持久化。**

# 状态 TTL 与内存模型笔记

## 概念说明

Flink 的 Keyed State 按 `keyBy` 后的 key 分布到不同 Subtask。状态后端决定这些状态主要如何保存：

- `HashMapStateBackend` 使用 JVM Heap 中的 HashMap 保存状态，读写路径短，适合状态量较小、低延迟和本地开发；状态增长会直接增加 Heap 压力，Full GC 和 OOM 风险更明显。
- `EmbeddedRocksDBStateBackend` 使用本地 RocksDB 保存 Keyed State。状态通过序列化读写，通常减少 JVM Heap 中的对象数量，适合状态量较大；代价是序列化、JNI、RocksDB MemTable/Block Cache、磁盘空间和 Compaction 的 CPU/IO 开销。
- 两种后端都可以参与 Checkpoint，但 Checkpoint 的状态大小、快照耗时和本地 IO 行为不同。RocksDB 并不意味着整个状态不占内存，也不意味着一定比 HashMap 更快。

TTL 是状态的过期策略。项目中使用的 Flink `StateTtlConfig` 按处理时间维护过期时间，适合清理“长时间没有被访问或写入”的在线状态；事件时间窗口和事件时间定时器仍负责业务窗口的结束，不能用 TTL 替代 Watermark 或 allowed lateness。

## 项目中的具体实现

### Join 状态

`ArticleJoinBehavior` 按 `article_id`（加盐统一作业中为内部 `article_id#salt`）进行双流关联。每个 Join key 维护：

| 状态 | 类型 | TTL/清理方式 | 作用 |
| --- | --- | --- | --- |
| `article` | `ValueState<JSONObject>` | 2 小时 TTL | 保存当前文章版本，行为先到时可以等待文章 |
| `pending-behaviors` | `MapState<String, JSONObject>` | 随文章补发或处理时间等待结束清理 | 保存文章尚未到达的行为，按 `event_id` 去重 |
| `pending-deadline` | `ValueState<Long>` | 处理时间定时器清理 | 记录这一篇文章待匹配行为的等待截止时间 |

文章到达后，Join 会补发 pending behavior；等待超时的数据写入 unmatched 旁路，后续由业务补算或离线核对处理。`reportLate` 只写 `late_data` 旁路，不直接丢弃仍可关联的迟到数据。

### Keyed State 与规则状态

- `RoleStreamUtil.prepare` 以 `event_id` 为 key 保存 `seen-event-id`，生产 TTL 为 24 小时，用于统一作业在 Checkpoint 回退、重放或多规则共享时做幂等去重。
- Join 前的行为 ETL 去重同样以 `event_id` 为 key，TTL 为 24 小时。
- Rule C 以 IP 为 key，维护窗口内点击记录和分钟窗口快照，两个状态的 TTL 都是 1 小时；事件时间定时器负责清理已经超过规则回看期的分钟数据，TTL 在 Watermark 停滞时提供兜底。
- Rule A、Rule B 的主要状态由事件时间窗口、Watermark 和允许迟到时间控制。窗口状态不是无限保留，窗口结束并超过允许迟到时间后才具备清理条件。

### 状态后端选择实现

统一作业 `Achieve_roleAll` 通过环境变量切换后端，其他业务逻辑保持不变：

```java
String stateBackend = System.getenv().getOrDefault("HOTNEWS_STATE_BACKEND", "hashmap");
if ("rocksdb".equalsIgnoreCase(stateBackend)) {
    env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
} else {
    env.setStateBackend(new HashMapStateBackend());
}
```

HashMap 基线使用消费组 `state-compare-hashmap-20261007`，RocksDB 使用 `state-compare-rocksdb-20261007`，两次运行不共用消费组，避免只消费 Kafka 尾部或混淆位点。RocksDB 依赖显式固定为 `frocksdbjni 6.20.3-ververica-1.0`，原因和排查记录见 [RocksDB-Windows原生依赖问题排查笔记.md](RocksDB-Windows原生依赖问题排查笔记.md)。

## 测试结果

### HashMap 基线

- Job ID：`9db3d11059be8c57d003295e27d79506`。
- 作业未从 Checkpoint 恢复，3 个 Slot 均使用。
- Checkpoint 共 11 次成功、0 次失败；状态大小从约 9.7 MB 增长到约 21.1 MB，后期轻载 Checkpoint 耗时降到几十毫秒。
- 后期 Join 三个 Subtask 的累计行为输入为 `35662 / 40239 / 23181`，最大值约占 40.6%；说明加盐后仍有一定分布差异，但已不是单个 Subtask 长期承受全部热点行为。
- 一次后期 TaskManager 采样：Heap Used 约 475 MB，Heap Max 约 3.27 GB，Direct Memory 约 73.7 MB，CPU Load 约 0.032；未观察到 OOM。
- Join 运行中采到的滚动 p95 曾约为 `50819 / 61718 / 33939 ms`。该指标包含排队和下游链路等待，不等同于纯 Join 函数耗时。

### RocksDB 运行

- Job ID：`35dd0a34687b84007ca056a72749aa8e`。
- 修复 JNI 原生依赖后作业成功进入 `RUNNING`，说明 RocksDB 后端可以在本机初始化；此前的 DLL 启动失败不计入性能结果。
- Checkpoint 结果：

| Checkpoint | 状态 | 状态大小 | 端到端耗时 | 确认子任务 |
| --- | --- | ---: | ---: | ---: |
| #1 | 成功 | 15.0 MB | 39.8 s | 42/42 |
| #2 | 成功 | 23.3 MB | 150.9 s | 42/42 |
| #3 | 成功 | 33.6 MB | 179.7 s | 42/42 |
| #4 | 失败，超时 | 5.4 MB | 180.0 s | 20/42 |
| #5 | 失败，超时 | 26.9 MB | 180.0 s | 28/42 |
| #6 | 失败，超时 | 6.7 MB | 180.0 s | 20/42 |

- 作业使用单个 TaskManager、3 个 Slot。一次后期采样：Heap Used 约 179 MB，Heap Max 约 3.27 GB，Direct Memory 约 69.5 MB，CPU Load 约 0.044，GC 累计时间约为 Full GC 127 ms、Young GC 1322 ms；采样期间没有 OOM。
- Join 三个 Subtask 的一次后期累计行为输入为 `19944 / 22271 / 11813`，滚动 p95 为 `131788 / 152683 / 47099 ms`。此时 Join 也受到下游反压，不能把 p95 全部归因于 RocksDB 读写。
- RocksDB 运行中 Checkpoint #4 至 #6 连续超时，说明在当前状态规模、JDBC Sink、单 TaskManager 和 180 秒超时配置下，RocksDB 的快照和下游排队存在稳定性风险。作业当时仍处于重试运行状态，不能把它描述为长期稳定。

### 结果口径

两次运行都使用了不同消费组和独立启动时间，Web UI 采样处于不同消费阶段；因此上述数字用于说明资源特征和 Checkpoint 风险，不计算 HashMap 与 RocksDB 的严格吞吐提升百分比。两次运行也没有改变 Join、Watermark、窗口或 Sink 逻辑，状态后端是主要实验变量，但 Kafka 起始位点和瞬时下游负载仍会影响指标。

## 问题和结论

### 观察到的问题

1. HashMap 基线的 JVM Heap 会随状态和窗口处理增长，本次没有出现 OOM，但 Heap 是主要风险边界。状态量继续增大、Key 数量增加或下游反压持续存在时，需要重点观察 Heap、GC 和 Checkpoint 时长。
2. RocksDB 明显减少了本次采样中的 JVM Heap 使用量，但引入了 JNI、磁盘和 Compaction 成本；Checkpoint 反而在状态增长和链路繁忙阶段接近或超过 180 秒。
3. Checkpoint 超时不能简单归因于 RocksDB。当前 Join 与 MySQL Sink 存在算子链，Rule B 还有单并行度窗口和 MySQL Sink；需要结合未确认 Subtask、busy、backpressure、Checkpoint 分阶段耗时和磁盘 IO 继续定位。
4. TTL 是按处理时间清理的闲置保护，不保证事件时间窗口立即删除，也不能阻止已经超过 Watermark 的数据进入 late 旁路。Join 的 pending behavior 还受处理时间等待定时器控制。

### 何时切换大状态后端

满足以下任一情况时，优先考虑 RocksDB 或其他大状态后端：

- 状态规模已经接近可用 JVM Heap，或者 Heap、Full GC 和 OOM 风险持续上升；
- 单个 Key 的 Map/List/窗口状态很大，HashMap 造成对象数量和 GC 压力；
- 需要通过外部化 Checkpoint 管理更大的状态，且本地磁盘和 IO 能够承受快照、恢复和 Compaction；
- 业务允许序列化读写带来的延迟，并且已经为 RocksDB 本地目录、磁盘空间、Checkpoint 超时和恢复时间做容量规划。

状态量较小、低延迟优先、开发机磁盘 IO 较弱或 Checkpoint 已经接近超时时，HashMap 更简单。不能只因为“状态可能变大”就盲目切换，应先用状态大小、Heap、GC、Checkpoint 时长和恢复时间做依据。

### 结论

本次实测证明：HashMap 在本项目当前数据规模下没有 OOM，Checkpoint 更稳定；RocksDB 能正常初始化并降低 JVM Heap 使用量，但在状态增长后 Checkpoint 连续超时，当前配置不适合直接宣称稳定。后续若要正式采用 RocksDB，应先处理 Checkpoint 超时和下游反压，配置可写且空间充足的本地/共享存储，并完成故障恢复和结果一致性验证。

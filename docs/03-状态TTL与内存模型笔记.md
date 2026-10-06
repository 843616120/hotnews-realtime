# 第四天：状态、TTL 与内存模型笔记

## 1. 考核目标与边界

第四天单独设计两个状态作业：

- `ArticleHeatStateJob`：记录文章在五分钟滑动窗口中的点击热度，并用热点文章制造文章键倾斜。
- `IpWindowStateJob`：以 IP 和一分钟不重叠窗口记录不同文章集合、点击数、阅读时长总和以及窗口起止时间，并用热点 IP 制造 IP 键倾斜。

这两个作业复用清洗、`event_id` 去重、Watermark 和 Join 输入，但不修改规则 A/B/C 的业务语义。文章作业沿用规则 A 的五分钟窗口、每分钟滑动和点击数大于 1000 的条件；IP 状态作业是一分钟不重叠窗口，不能把它和规则 C“每次点击回看上一分钟”的逐点击语义混为一谈。

TTL 是处理时间上的状态失效策略，不是事件时间窗口关闭条件，也不是迟到数据处理策略。窗口关闭仍由 Watermark、允许迟到时间和定时器决定；超过迟到边界的数据必须进入可观测旁路并继续尝试补算，不能用 TTL 静默丢弃。

## 2. 状态结构、字段和用途

| 状态名 | 类型 | Key/字段 | TTL | 业务作用 |
| --- | --- | --- | --- | --- |
| `event_id` 去重标记 | `ValueState` | 行为 `event_id` | 24 小时 | ETL 在 Join 前去重，避免重复行为重复计数；超出保留期不承诺永久去重。 |
| `day4-article-shards` | `MapState<String, ClickAccumulator>` | `window_start:shard` | 窗口结束后清理 | 第二阶段按文章合并 16 个分片的点击累计值，避免热点文章全部落在一个 Subtask。 |
| `day4-article-latest-heat` | `ValueState<JSONObject>` | `article_id` | 24 小时 | 保存每篇已输出热点文章的最近结果，便于读取最近状态；不替代窗口状态。 |
| `day4-ip-window` | `ValueState<IpWindowState>` | `ip:window:shard` | 1 小时 | 第一阶段保存一个 IP、一个窗口、一个分片的局部集合、点击数、时长总和和窗口起止。 |
| `day4-ip-partials` | `MapState<String, IpWindowState>` | `window_start:shard` | 1 小时 | 第二阶段暂存 IP 的各分片结果，合并集合和计数。 |
| `day4-ip-latest-window` | `ValueState<IpWindowState>` | `ip` | 1 小时 | 保存最近完整窗口快照。 |
| `day4-ip-last-alert` | `ValueState<Long>` | `ip` | 1 小时 | 记录最近已经输出的告警窗口，防止同一窗口重复告警。 |

`IpWindowState` 的核心字段如下：

```text
ip             IP 地址
articleIds     当前窗口内去重后的文章 ID 集合
clickCount     当前窗口点击总数
durationSum    当前窗口阅读时长总和，单位毫秒
windowStart    窗口起始事件时间
windowEnd      窗口结束事件时间
```

文章 `ClickAccumulator` 还保存点击数、文章版本、最新文章维度时间、标题和分类。合并时点击数相加，文章维度取版本更高或同版本事件时间更新的值，保证加盐前后输出字段一致。

## 3. TTL 选择和清理频率依据

### 3.1 文章热度 TTL：24 小时

文章热度需要支持全天内的重放、迟到修订和最近热度查询。设置 24 小时的理由是：

1. 规则窗口允许窗口结束后的迟到更新，状态至少要覆盖一个业务日内的补算周期。
2. 热度快照只保存最近一次已输出结果，不保存无限历史；无新结果 24 小时后没有继续在线修订价值。
3. 24 小时过期不会关闭事件时间窗口，也不能代替 Watermark 定时器。

窗口分片状态在“窗口结束 + 30 秒允许迟到”后的事件时间定时器中主动合并并删除。`day4-article-latest-heat` 使用处理时间 TTL 24 小时，按访问/更新语义失效；Flink TTL 到期不保证物理空间在同一时刻立即回收，Checkpoint、RocksDB compaction 或 JVM 回收会影响实际占用。

### 3.2 IP 状态 TTL：1 小时

IP 状态只服务一分钟窗口和最近窗口查询。1 小时覆盖 60 个窗口长度，能够容纳短时间停更、迟到修订和重试，同时避免异常 IP 的集合状态长期驻留。窗口在结束加 30 秒后输出/合并；1 小时 TTL 是没有后续访问或更新时的兜底清理，不替代窗口定时器。

如果生产数据量明显不同，TTL 需要按以下公式复核，而不是凭经验缩短：

```text
状态条数 ≈ 输入速率 × TTL × 去重后保留比例
状态空间 ≈ 状态条数 × (序列化字段大小 + 后端索引/对象开销)
```

示例估算（仅用于容量规划）：

| 场景 | 假设 | 估算 |
| --- | --- | --- |
| 行为去重 | 100000 条/小时、每条事件 ID 与状态开销按 80 B、24 小时 | 约 192 MB，未计 JVM/后端索引开销 |
| 高峰去重 | 2000 条/秒、24 小时、每条按 80 B | 约 1.728 亿条、约 13.8 GB，不能放入普通 JVM 堆而不评估后端 |
| IP 集合 | 120000 个不同文章 ID/分钟、每个 ID 与集合开销估计 80 B | 约 9.6 MB/窗口，再加分片和序列化开销 |
| 文章快照 | 500 篇、每份约 1 KB | 约 0.5 MB，实际内存需测量 |

因此当前 24 小时和 1 小时不修改。若要缩短 TTL，必须同时给出生产速率、唯一键比例、迟到/补算最大跨度、预计状态量和清理频率，并用固定批次回归确认不影响结果。清理频率的最低依据是：事件时间窗口在窗口结束加 30 秒结算一次，TTL 每次状态访问时判断过期，Checkpoint/后端 compaction 负责物理空间逐步回收。

## 4. Key 倾斜、加盐与两阶段聚合

### 4.1 优化前

文章原始 Key 为 `article_id`，热点文章的 90% 输入全部被路由到一个 Subtask；IP 原始 Key 为 `ip`，热点 IP 同样集中到一个 Subtask。即使并行度为 3，增加并行度也不能把同一个 Key 的处理拆开，表现为一个 Subtask 计数远高于其他 Subtask。

### 4.2 优化后

两类状态都使用事件 ID 的稳定哈希分片：

```text
article first key = article_id + "#" + floorMod(hash(event_id), 16)
ip first key       = ip + "#" + floorMod(hash(event_id), 16)
```

第一阶段按 `article_id + shard` 或 `ip + shard` 保存局部状态，第二阶段重新按文章或 IP 聚合：

1. 计数相加。
2. IP 的 `articleIds` 做集合并集。
3. 阅读时长做总和，并用 `durationSum / clickCount` 计算平均时长。
4. 窗口起止和文章维度字段保持一致。
5. 只在窗口结束加 30 秒后输出最终状态；同一窗口的迟到更新覆盖此前局部累计值，不静默丢弃。

加盐不能消除第二阶段的聚合成本，也不能保证吞吐一定上升；验收必须同时检查 Subtask 分布、p95、吞吐和最终结果一致性。

## 5. HashMap 与 RocksDB 状态后端

### 5.1 Memory/HashMap

Flink 1.17 的 HashMap/HashMapStateBackend 将工作状态主要保存在 TaskManager JVM 堆内。访问路径短、序列化和本地小数据实验开销较低，适合状态量已知且可稳定放入堆的固定批次。缺点是状态随堆增长会增加 GC 压力，Checkpoint 通常需要处理较大的内存快照，无法用磁盘自然承接大规模状态。

### 5.2 RocksDB

EmbeddedRocksDBStateBackend 将 keyed state 存在本地 RocksDB，并使用 Flink 托管内存和本地磁盘承接更大的状态。它适合高基数 `event_id` 去重、IP 文章集合增长和需要增量 Checkpoint 的场景。代价是序列化、JNI、磁盘 I/O、compaction 和增量快照有不同成本，延迟不一定优于 HashMap。

作业和测试入口都支持：

```text
ArticleHeatStateJob --backend=hashmap|rocksdb
IpWindowStateJob   --backend=hashmap|rocksdb
-Dday4.backend=hashmap|rocksdb
Maven profile: -Pday4-rocksdb
```

### 5.3 Docker Linux 实测

Windows 本地仍可能因为 `librocksdbjni-win64.dll` 的依赖问题无法启动 RocksDB；正式补采改在 Flink Linux Docker 集群完成。固定输入为 500 条文章、100000 条行为，3 个并行度，5 秒一次 Checkpoint，8 个 TaskManager Slot。每次使用唯一 `--run-id` 消费组，只有完整读取 100000 条行为且无重启的样本才纳入正式证据。

有效证据位于 `tests/day4/evidence/`，四组作业各有 HashMap 基线/加盐和 RocksDB 基线/加盐：

| 作业 | 后端 | 模式 | 吞吐 events/s | TaskManager 峰值 | RocksDB 峰值 | Checkpoint |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 文章热度 | HashMap | 基线 | 10385.29 | 955.9 MB | 0 | 2/2 成功 |
| 文章热度 | HashMap | 加盐 | 7276.43 | 1060.9 MB | 0 | 3/3 成功 |
| 文章热度 | RocksDB | 基线 | 933.58 | 1173.5 MB | 4.61 MB | 7/7 成功 |
| 文章热度 | RocksDB | 加盐 | 679.34 | 1226.8 MB | 5.17 MB | 6/7 成功，1 次失败 |
| IP 状态 | HashMap | 基线 | 6478.36 | 1208.3 MB | 0 | 3/3 成功 |
| IP 状态 | HashMap | 加盐 | 9659.97 | 1297.4 MB | 0 | 2/2 成功 |
| IP 状态 | RocksDB | 基线 | 1156.35 | 1361.9 MB | 4.55 MB | 7/7 成功 |
| IP 状态 | RocksDB | 加盐 | 1417.78 | 1353.7 MB | 4.60 MB | 6/7 成功，1 次失败 |

上述吞吐是 `100000 / 作业墙钟耗时`，用于同环境相对比较，不是持续生产上限。RocksDB 的速度低于 HashMap 属于本次固定批次的实测现象，原因应从序列化、JNI、磁盘 I/O 和 compaction 解释，不能据此断言所有生产负载都相同。加盐也不保证吞吐上升：文章在两个后端均变慢，IP 在本次样本中上升。

正式 Docker 作业没有 Source-to-keyed Probe，因此 `p95_source_to_keyed_ms` 为 `null`；不能把 Checkpoint 时长、busy time 或 backpressure time 冒充 p95。p95 和 Subtask 分布沿用本地 `Day4SkewBenchmark` 的显式探针证据，并与 Docker 资源表分开解释。文章 RocksDB 加盐和 IP RocksDB 加盐各有 1 次 Checkpoint 失败，作业本身最终完成，报告不能写成“全部成功”。

### 5.4 何时切换大状态后端

满足以下任一信号时，应在隔离批次上评估切换 RocksDB：

- 去重键或 IP 集合的估算大小接近可用 JVM 堆，导致频繁 Full GC 或 OOM 风险；
- HashMap 全量 Checkpoint 持续过大，Checkpoint 时长超过业务允许的恢复间隔；
- 状态规模随流量持续增长，无法通过合理 TTL、分片和业务保留期控制；
- 能提供足够本地磁盘和托管内存，并可接受访问、序列化和 compaction 开销。

切换前要用同一批输入比较两种后端的资源和结果，不能因为设置了 TTL 就假设 JVM 或磁盘会立即释放，也不能只因吞吐更高就切换。

## 6. 类名、用途与思路

| 类名 | 用法 | 思路 |
| --- | --- | --- |
| `ArticleHeatStateJob` | 常驻运行；`--optimized` 开启加盐；`--bounded` 仅用于固定批次验收；`--backend=...` 选择后端 | 复用 Join 后的行为流，输出规则 A 语义的文章热点。 |
| `ClickAccumulator` | 文章第一阶段聚合对象 | 累加点击，并保留最新文章版本、标题和分类。 |
| `ShardWindow` | 文章窗口函数 | 输出窗口起止、分片号和局部累计值。 |
| `MergeShards` | 文章第二阶段聚合 | 按文章合并 16 个分片，窗口结束加 30 秒后按大于 1000 输出。 |
| `RememberHeat` | 文章最新热度状态 | `ValueState` 保存最近热点，TTL 24 小时。 |
| `IpWindowStateJob` | 常驻运行；`--optimized` 开启 IP 加盐；`--backend=...` 选择后端 | 统计一分钟不重叠 IP 窗口，并输出可合并的完整状态。 |
| `IpWindowState` | IP 局部或完整状态 | 保存 IP、文章集合、点击数、时长总和和窗口起止。 |
| `CountIpShard` | IP 第一阶段 | 以 IP 分片为 Key 聚合并注册事件时间定时器。 |
| `MergeIpShards` | IP 第二阶段 | 合并分片集合、计数和时长，按相同阈值输出。 |
| `RoleStreamUtil` | 规则入口和 Day 4 入口共用 | Join 后二次幂等保护并恢复行为事件时间。 |
| `Day4SkewBenchmark` | 显式执行本地基准 | 记录每个 Subtask 数量、吞吐和 Source 到 keyed 探针 p95，并断言前后结果相等。 |

## 7. 结论

状态字段、TTL、热点加盐和两阶段聚合已经落地；HashMap 优化前后本地结果一致，热点 Subtask 分布明显改善。RocksDB 已在 Linux Docker 集群完成 4 组后端对照，文章和 IP 的加盐样本各有 1 次 Checkpoint 失败；因此资源对照已具备证据，但生产切换前仍需继续观察 Checkpoint 稳定性。

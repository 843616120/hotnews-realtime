# 第四天：状态、TTL 与倾斜实验

## 概念与实现

`ValueState` 保存一个键的最新值（去重标记、IP 窗口、文章热度快照）；
规则 C 的 `MapState` 保存同一 IP 的点击及每分钟告警版本；第四天的
`MapState` 在文章或 IP 键下按 `window_start:shard` 保存分片累计结果。
TTL 是**处理时间**的失效策略，
不是事件时间窗口的关闭条件。规则 A/B/C 的修正状态采用**事件时间**
窗口结束后 24 小时清理；第四天独立状态作业仍按各自窗口和 TTL 策略结算；
TTL 到期不保证物理空间立即回收。重放超过 24 小时的同一 `event_id` 可能再次被计数，
长期归档或离线补数必须另有幂等键。

处理链路：Schema ETL -> 按 `event_id` 去重（24h）-> Watermark ->
首版时间时序 ETL / Join 富化 -> 规则入口的二次幂等保护与时间恢复 ->
各自独立运行 A/B/C、`ArticleHeatStateJob` 或 `IpWindowStateJob`。
A/B 按文章键保存可修正窗口状态，C 按 IP 保存明细与可更新/撤销的告警。
第四天的独立状态作业仍与 A/B/C 分开，不把规则窗口的事件时间保留期误称为
第四天状态的处理时间 TTL。
独立文章作业优化模式的五分钟分片窗口到窗口结束加 30 秒清理，
未优化模式复用规则 A 的 24 小时事件时间修正状态；
`day4-article-latest-heat` 单独保存每篇**已告警**文章的最近热度，
24h 无更新后失效，不把五分钟窗口误称作 24h 状态。
IP 作业的 `day4-ip-window` 按 IP、窗口起点及分片记录不同文章集合、
点击数、阅读时长总和和窗口起止；合并阶段保存最近的完整窗口快照。
这些 IP 状态统一设 1h TTL。计数在窗口结束加 30 秒结算，`article_ids[]`
排序后输出。**它是一分钟不重叠窗口，原规则 C 是每次点击回看一分钟：
两种定义的输出时间和边界不同，不能互换或共用 C 的 SQL 对照。**

## 类名、用途与思路

| 类名 | 怎么用 | 实现思路 |
| --- | --- | --- |
| `ArticleJoinBehavior` / `BehaviorDeduplicate` | 规则作业调用 `createJoinedStream`；`--bounded` 只做固定批次验收 | Schema 清洗后 ETL 去重；按流生成 Watermark；先到行为立即未匹配、保留待首版文章补 Join，迟到留痕后仍继续 Join。 |
| `RoleStreamUtil` | 每个作业在 `buildRule` 前调用 `prepare` | 在生产 ETL 已去重的前提下做二次幂等保护，再按行为 `event_time` 恢复 Watermark；测试用短 TTL 验证失效。 |
| `Achieve_roleA`、`ClickAccumulator`、`CountClicks` | 原有热点文章规则，不带 `--optimized` | 五分钟滑动窗口，每分钟滑动；按文章键保存 24 小时事件时间修正状态，晚到点击可补发过阈值结果。 |
| `ArticleHeatStateJob` | 独立启动；追加 `--optimized` 启用加盐 | 原始模式复用规则 A 输出；优化模式先按事件 ID 分 16 片再按文章合并，都保存最近热度 24h。 |
| `ClickAccumulator`、`CountClicks` | 文章作业优化模式的聚合对象和增量函数 | 统计次数，按版本及事件时间保留最新文章维度；`merge` 合并分片。 |
| `ShardWindow` | 文章作业第一阶段的窗口输出 | 按分片输出五分钟滑动窗口累计值。 |
| `PartialClicks`、`MergeShards` | 分片消息及第二阶段合并算子 | 用文章键重聚合，迟到更新覆盖同一片的旧累计值；待上游窗口清理后合并并删除分片状态。 |
| `RememberHeat` | 独立文章作业两种模式的输出端共用 | `ValueState` 保存最近的热点结果，处理时间 TTL 24h，与规则 A 的事件时间保留期不同。 |
| `Achieve_roleB`、`ArticleScore`、`RankCategories` | 分类热度规则 | 按文章累计，窗口键重算榜单，晚到更新可覆盖旧文章分数。 |
| `Achieve_roleC`、`CandidateAlert`、`DetectSuspiciousIp` | 逐点击回看刷量规则 | 按 IP 保存点击与最早告警；晚到点击按分钟更新或撤销，24 小时事件时间清理。 |
| `IpWindowStateJob` | 独立启动；追加 `--optimized` 按 IP 加盐 | 原始模式一片，优化模式 16 片，两者都先算一分钟局部状态再合并、按相同阈值告警。 |
| `IpWindowState`、`IpPartial` | IP 作业的完整状态和单分片输出 | 保存不同文章集合、点击数、时长总和与窗口起止；分片只传累计指标。 |
| `CountIpShard`、`MergeIpShards` | IP 作业的两阶段算子 | 前者按事件时间累计并注册定时器；后者合并集合及计数，输出快照与告警。 |
| `Day4SkewBenchmark` | 仅显式 `-Dtest=Day4SkewBenchmark` 运行 | 分别预热并交错测文章/IP 两组原始与优化路径，断言各组最终结果相等。 |
| `Stamp`、`Probe`、`ProbeResult` | 基准内部的打点、分组探针和读数 | 每个 Subtask 记录处理量及源到探针的时延样本，不混充端到端告警时延。 |

## 数据量与 TTL 估算

固定样本为 10 万行为/小时，若 `event_id` 平均按 80 B/条估算（包括键、标记与
状态开销，实际以后端指标为准），24h 去重约 `100000 * 24 * 80 = 192 MB`；
若持续 2000 events/s，24h 会达到 1.728 亿条，按同样假设约 13.8 GB。
不能以固定样本的体量推导生产容量。原 C 若峰值 2000 次/s 均属同一 IP，
按 500 B/条保存 JSON，则一分钟约 60 MB 且每次定时器扫描明细；
独立 IP 作业按不同文章集合聚合，若一分钟 12 万篇不同文章、每个 ID
及集合开销估计 80 B，约 9.6 MB，再加分片和序列化开销；实际需测量。
1h TTL 是停更 IP 的兜底期限；Watermark 停滞时事件时间定时器不能及时清理。
文章热点快照按 500 篇、每份 1 KB 约 0.5 MB；实际内存需测量堆、
状态大小、Checkpoint 时长及 GC。

HashMapStateBackend 将工作状态留在 JVM 堆上，较适合此固定小样本；
EmbeddedRocksDBStateBackend 把工作状态置于本地磁盘并使用托管内存，
适用于去重键大量增长，但访问、序列化和增量快照有不同成本。
两个独立作业都支持 `--backend=hashmap|rocksdb`；本地 IDE 使用 RocksDB
可启用 Maven `-Pday4-rocksdb` 添加 Flink 1.17 对应运行依赖，
生产 Flink 分发包需包含后端 JAR。性能基准另支持
`-Dday4.backend=hashmap|rocksdb`，输出文件包含后端名称。
**本机没有可用 Docker 集群；RocksDB Maven 依赖已下载，但 Windows
`librocksdbjni-win64.dll` 报 `Can't find dependent libraries`，作业未启动，
所以没有 RocksDB 内存、磁盘、GC 或 Checkpoint 实测数值。**复现命令见
`tests/day4/README.md`；关键异常链为
`JobExecutionException -> Could not restore keyed state backend ->
Could not load the native RocksDB library -> UnsatisfiedLinkError`。
Maven 的 `target/surefire-reports` 会被下次运行覆盖，不能作为长期验收证据；
依赖缺失或原生库错误也不能当作性能结果。
要比较两种后端，应保持相同输入、
并行度、Checkpoint 配置与硬件，分别采集堆、托管内存、RocksDB 磁盘、
Checkpoint 大小/时长、GC、吞吐及 p95，再判断是否切换。
当去重键与 IP 状态逼近可用堆、GC 或全量快照持续成为瓶颈，同时有足够
本地磁盘/托管内存并可接受访问开销时，再以同批数据测试迁移至 RocksDB；
不能只因设置 1h/24h TTL 就假设物理内存会立刻释放。

## 本地实测与结论

主报告在 `tests/day4/results/local-skew-hashmap-2026-10-05T02-52-21.036Z.json`；
之前一轮在 `local-skew-2026-10-05T02-44-13.756Z.json`，可观察本机噪声。
每组 12000 条输入，90% 落在一篇文章或一个 IP，三并行度，两轮交错测试：

| 热点键 | 原始 Subtask 分布 | 加盐 Subtask 分布 | 原始吞吐 events/s | 加盐吞吐 events/s | 原始 p95 ms | 加盐 p95 ms |
| --- | --- | --- | --- | --- | --- | --- |
| 文章 | 480/11160/360 | 5681/2426/3893 | 9803/10205 | 9061/9665 | 18.68/21.81 | 4.63/4.03 |
| IP | 11520/360/120 | 3796/3759/4445 | 9353/10309 | 9570/9186 | 6.59/29.29 | 4.19/3.37 |

两组原始与优化输出均逐字段一致（文章五个窗口，IP 一个告警窗口）。
加盐**改善分布，但吞吐无稳定提升**；本轮探针 p95 下降，
上一轮文章和 IP 的结果则不一致地波动。这份小样本的
作业启动与本机噪声影响明显，不能声称优化达到生产性能目标。
`p95_source_to_keyed_ms` 仅从 Source 打点到分组探针，不是告警端到端延迟。

当前规则测试从 `flink-job/pom.xml` 启动：`mvn -o -f flink-job/pom.xml
-pl flink-rolesachieve -am test`，10 个 Java 用例通过。上述倾斜测量
为修改规则 A/C 之前的历史数据，不能当作本轮规则吞吐结论。
`node --no-warnings --test tests/roles/baseline.test.js`：1 个独立 SQL 用例通过。
本机没有 Docker，Kafka 的有界双 Topic 实跑及 RocksDB 对照尚未验收。

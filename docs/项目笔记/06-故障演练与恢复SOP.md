# 06 故障演练与恢复 SOP

## 适用范围与原则

仅用于隔离的本地 Compose/Flink 项目环境。故障处理顺序是**先止损、再保数、后恢复、最后复盘**：先避免两个同消费组或同目标表的作业同时写入，保住 Kafka 原始 Topic、最近成功的 Checkpoint/Savepoint 和数据库证据，再做最小动作恢复。不要清卷、删快照、向业务 Topic 注入虚假未来事件或因为页面显示 `RUNNING` 就宣布数据已恢复。共享 TaskManager 可能同时承载其他作业，注入故障前须确认维护窗口和影响范围。

## 现场预检与止损

1. 记录本次输入 Topic/分区末尾位点、消费组、作业 ID/JAR 版本、运行配置、当前所有作业、最后成功 Checkpoint ID/path/大小/时长、Kafka Lag；冻结同表和 Redis Key 的其他写入/回放作业。
2. 留存 [Day 5 采样器](../../tests/day5/README.md)的 MySQL 五表计数、A/B 逐业务键值、C 活跃与撤销、Redis 窗口/修订号/完整榜单/TTL，以及异常表未解决事件。无法登录 MySQL 时标记为缺失，不能用 Redis 或 Flink 代替。
3. 先看根因：失联 TaskManager、Checkpoint 对齐超时、Source 位点/Watermark 停滞、Sink 重试、状态后端恢复或资源膨胀。暂停破坏性操作，检查 Kafka retention 是否仍覆盖待回放时间。

## 恢复路径

| 故障 | 定位证据 | 处理与数据风险 |
| --- | --- | --- |
| TaskManager 中断 | Flink vertices、日志、`latest.restored`、新成功 Checkpoint | 在维护窗口恢复原服务，等待所有顶点运行且新快照成功；外部已提交但快照未成功的批次可能被重写 |
| 空闲分区拖住 Watermark | 各 Source 当前 Watermark、分区活跃性、末端窗口 | 检查 `withIdleness(30s)` 与实际输入；全流停更时用隔离有界回放确认最终窗口，不伪造时间 |
| Sink 变慢/超时 | 下游 busy、上游 backpressured、Kafka Lag、JDBC/Redis 错误和快照时长 | 限制新流入或调合适批量/并行度，保证 Kafka 保留足够；先记录业务输出再变更配置 |
| 状态增长/OOM | 堆/GC、RocksDB 磁盘、状态大小、恢复栈 | 保存最近快照与日志，降低压力并评估后端/TTL；不得盲目缩 TTL 丢可补算状态 |
| 旧快照回放 | 原/新 Job ID、实际 `_metadata`、业务键对照 | 先停止唯一旧作业，再用存在且兼容的快照启动；警惕 C 撤销倒退、Redis Key 过期后旧版本覆盖 |

[可直接执行的命令与检查点](../04-Day5-Sink与恢复验收.md)说明了 `flink list`、REST 快照、Kill/重启 TaskManager、Savepoint 创建和恢复、SQL/Redis 核验。这里只规定顺序和判据，不填历史 Job ID 作为今天可用路径。不得在原作业运行时启动 `--bounded` 往同一张表/同一个 Redis Key 回放。

## 已做的演练与验收判定

[10 月 6 日现场记录](../06-Day5-Checkpoint与Savepoint记录.md)已有一次 TaskManager SIGKILL：从 `chk-19` 恢复后 11/11 顶点运行并有 `chk-31`；前后 MySQL 97044 条明细、A 105、B 25、C 活跃 0，A/B 逐键和 Redis Top 5 未回退；异常表多 18 条。Savepoint 以调整 JDBC 批大小后恢复，新 Job 有成功 `chk-72`，但首次提交遇到 JDBC 驱动错误并经修复重打包才恢复；Savepoint 后 MySQL 逐键对照**未完成**。不能把一次 Kill/Savepoint 流程说成 PDF 要求的“至少两类随机故障、30 分钟内完全定位并核对”已通过。

补演练时，从“发现”开始计时，分别记止损、快照确认、恢复提交、Kafka Lag 追平、MySQL/Redis 校验的时间；至少选择另一类隔离故障（如慢 Sink/Checkpoint 超时或 Watermark 空闲），逐项记录**现象 - 证据 - 定位 - 处理 - 回归**。规则输出按窗口键比较，不拿常驻未关窗的 A 105/B 25 强行等同有界 A 123/B 60。完成后附 REST JSON、Flink 日志、SQL 快照和监控截图；缺一项就标明范围。

**结论：**已有单次真实 TaskManager 自动恢复与一次有条件 Savepoint 恢复证据，不能推断任意旧快照、全部外部 Sink 和所有故障类型均幂等。恢复的最终判据是输入追平、窗口/告警逐键核对与新成功 Checkpoint，而不是进程存活。

# 02 事件时间与 Watermark 笔记

## 概念说明

Watermark 是事件时间进度估计，不保证更早事件绝对不会再来，也不等于机器时间；窗口允许迟到时间从窗口结束时算起。慢算子通常拖慢 Watermark 的传输，不能简单解释成“反压把 Watermark 推快”。但不同 Kafka 分区、文章/行为输入和 Join 输出的进度不一致时，旧行为可能在某条 Watermark 已前进之后才进入规则。

## 项目中的具体实现

- `ArticleJoinBehavior` 分别按文章和行为的 `event_time` 分配 30 秒乱序 Watermark，两侧均设置 30 秒空闲检测；Join 按 `article_id` 暂存先到的行为，文章到达时补发。`RoleStreamUtil.restoreBehaviorTime` 恢复补发记录的行为事件时间，但不重新生成或约束 Join 的输出 Watermark。
- A/B 窗口允许迟到 65 分钟；超过时只进入旁路，不再参与对应窗口。C 用 Watermark 与分钟回看清理时刻比较，过期部分也只留痕。Join 自身的 `late_data` 只留痕并继续关联，不能代替规则窗口的补算。
- A 用 5 分钟滑窗、步长 1 分钟，B 用 10 分钟滚窗；当一个输入记录所有所属窗口均已超期时，分别进入 `ROLE_A_LATE`、`ROLE_B_LATE` 的 Side Output；C 的一分钟回看过期进入 `ROLE_C_LATE`。统一作业把三类审计写入 MySQL `pipeline_event` 并打印；Join 的迟到数据单独写入 `late_data` 表。A 的一条点击参与多个正常窗口，但不能据此断言它会在迟到旁路中打印多次；跨次重跑、日志追加或重复输入仍需核对 event_id 才能得到唯一行为数。
- 生成器的文章事件时间分布在首小时，每条行为再取文章发布时间后的 1~3600 秒；因此 `total_data` 中有效行为的**整体事件时间跨度接近两小时**。生成器前 1% 的先到行为还可能将到达时间提前至文章发布后 1~4 秒，而保留远在未来的行为事件时间；“5~30 秒乱序”不能覆盖这个特殊分支。

## 测试结果

- 对 `generator/generator/total_data` 的合法 JSONL 进行只读统计：文章事件时间 `2026-09-27T00:00:07.633Z` 至 `00:59:55.525Z`；行为事件时间 `00:00:08.860Z` 至 `02:00:04.516Z`。按文件中 `ingest_time` 顺序统计的最大事件时间回退约 60.4 分钟；这不是整体事件时间跨度，也不是 Kafka 实际多分区消费顺序。
- 历史 `tests/roles/sql-check/logs/role-all.log` 中，C 的迟到行为 `behavior-event-00017398` 事件时间为 `00:06:25.155Z`，收到时 Watermark 为 `01:47:47.188Z`，相差约 101.4 分钟。该日志的 A/B/C 迟到打印分别约为 50,013 / 52,336 / 54,637 条。日志可能包含此前运行阶段，不把打印总行数当作单次实验的精确输入比例。
- 这说明规则收到该记录时已超过 65 分钟范围；但仅凭日志无法区分 Kafka 分区排队、Join 暂存补发和下游反压分别贡献了多少时间差。
- 新版**仅加测量指标、尚未做倾斜优化**的统一作业 `14570ee6466c1c734943dee2b1e2c7db` 在 Web UI 中显示：文章侧 Watermark 最后值约 `00:59:25.524Z`，Join 行为侧和输出 Watermark 约 `01:59:30.915Z`。文章流读完后变为空闲输入，不能把 `currentInput2Watermark` 最后值当成当前仍在限制输出水位线的值。同时行为 Kafka Source 仍有数万条级的 `records-lag-max` 样本；Join 第 2 号 Subtask 约 27,080 条直接匹配对 61 条补发。这比“全是等文章补发造成”的解释更符合本次现场。
- 该 Job 运行中日志的一次截面：首条 `ROLE_C_LATE` 的事件时间 `00:01:06.736Z`，`watermark_ms` 对应 `01:37:38.187Z`，差约 96.5 分钟；仍须区分行为源各 Kafka 分区的事件时间先后、热 key 排队和同链 Sink 等待，不能仅凭这个值断言 Watermark 算法错误。
- 加盐版 Job `b2605ced59d7e4cffbf1f6e17c0d865f` 的运行中日志仍有大量规则迟到；一个 C 样例事件时间 `00:08:31.155Z`、Watermark `01:59:30.915Z`，相差约 111 分钟。加盐不修改 Watermark 设置；本轮不能因 Join 热点缓解就宣称 A/B/C 结果与 SQL 已一致。下一步应单独跟踪 Kafka 分区旧事件何时到达、B 单并行度下游反压及规则超期补算。
- 本次是同一批 Topic 的运行现场观测，**不是**人为暂停单个 Kafka 分区、手动注入窗口边界数据的受控实验。文章 Source 约 500 条读完后，Join 的文章侧最后 Watermark 停在 `00:59:25.524Z`，输出约 `01:59:30.915Z`；该现象与 30 秒 `withIdleness` 使文章侧不再阻挡活跃行为侧吻合，但未逐分区捕获 idle 标记及转换前后瞬间，不能凭两个 Watermark 数字单独证明具体哪个 Kafka 分区在何时进入 idle。超过 65 分钟的 Side Output 已从控制台确认；恰好处于窗口边界、允许迟到边界前/后的独立注入测试尚未做。

### HashMap 与 RocksDB 对 Watermark 的影响

- 状态后端不直接生成 Watermark，也不改变 `event_time`、30 秒乱序参数或 65 分钟允许迟到时间。项目的 Watermark 仍由 Source 根据已经处理到的最大事件时间计算：`最大 event_time - 30 秒`。
- 状态后端会改变算子处理速度。RocksDB 的状态序列化、JNI、Checkpoint 和本地磁盘 IO 可能使算子变慢，进而让下游反压；反压后 Kafka Source 读取新记录的速度下降，Source 观察到的最大 `event_time` 推进变慢，Watermark 也就更晚推进。
- Watermark 是“已经处理到哪里”的事件时间进度估计，不是独立于数据处理速度的机器时钟。因此 RocksDB 运行中即使事件时间字段完全没有改变，也可能出现 Watermark 更低、规则判断为 late 的时机更晚，表现为 late 数据明显减少。
- 本次 RocksDB Job `35dd0a34687b84007ca056a72749aa8e` 的现场中，Join 子任务出现明显反压；RocksDB Checkpoint #2、#3 分别耗时约 150.9 秒、179.7 秒，#4~#6 又在约 180 秒超时。该运行中 Join 三个 Subtask 的后期 p95 约为 `131788 / 152683 / 47099 ms`。这些数据说明处理和快照确实可能拖慢事件进度，但不能证明 RocksDB 改变了 Watermark 算法。
- 对照 HashMap Job `9db3d11059be8c57d003295e27d79506`，其后期 Checkpoint 共 11 次成功、0 次失败，规则运行阶段与 RocksDB 不同。两次使用不同消费组和启动时刻，不能仅按 late 数量计算后端优劣；需要在相同 Kafka 位点、相同生产速率下同时记录 Source/Join 的 Watermark、反压和 Lag。
- `RoleStreamUtil.restoreBehaviorTime` 只恢复补发行为记录的事件时间，并原样转发上游 Watermark，不会在 Join 后重新生成 Watermark。因而观察到的差异首先应从 Source 消费速度、算子反压和 Watermark 传输延迟解释。

## 问题和结论

- 当前不能把“恢复了行为事件时间”理解为“输出顺序也满足原来的 30 秒乱序假设”；先到行为被暂存后，规则可能看到事件时间落后于已转发的 Watermark。
- 优化前后都应按同一批 Kafka 位点记录各分区消费进度、Join 输入/输出 Watermark、补发计数和规则 late 数量，再判断是否需要调整生成器的先到分支、Join 输出时间语义或设计规则超期补算。仅增大允许迟到时间会增加状态和窗口输出延迟，不能代替诊断。
- 统一作业已加入 Join 前加盐以分散热点 key，但未更改事件时间/Watermark；优化后再次观测到大量 A/B/C late，**原问题尚未解决**。接下来须用同一 `event_id` 追踪 Kafka 分区输入、Join 输入和规则入口的时间戳/Watermark，再决定是调整时间进度策略还是增加真正的超期补算；不可将加盐或继续增大 65 分钟视为验收完成。
- 本次 RocksDB 运行中 late 数据变少，合理解释是 RocksDB/Checkpoint/下游反压使 Source 读取和 Watermark 推进变慢，导致记录在 Watermark 超过允许范围前被处理；这属于处理进度变化，不是 RocksDB 修复了事件时间逻辑。后续验收仍应以相同输入位点下的 Watermark、Lag、反压和结果一致性为准。

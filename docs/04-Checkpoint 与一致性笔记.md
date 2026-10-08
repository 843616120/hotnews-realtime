# Checkpoint 与一致性笔记

## 当前配置与依据

规则、Join 与状态作业通过 `FlinkRuntimeUtil.configureCheckpointing` 配置 Checkpoint。统一规则作业单独采用 30 秒间隔、180 秒超时；其余入口沿用 5 秒间隔、60 秒超时。两套配置都使用 EXACTLY_ONCE、最小间隔 2 秒、最大并发 1、容忍连续失败 3 次，以及失败后最多重启 3 次、每次等待 5 秒。

| 参数 | 统一作业取值 | 依据 |
| --- | --- | --- |
| 存储目录 | 优先 `HOTNEWS_CHECKPOINT_DIR`；本地未设置时为运行工作目录下 `checkpoints/` | IDEA MiniCluster 不再将快照仅存 JobManager 内存；Docker 通过 Compose 指向已挂载的 `/opt/flink/checkpoints/day5`。本地磁盘不等于跨机器共享存储。 |
| 间隔 | 30 秒 | 本地统一作业第一份快照就耗时 24.3 秒；5 秒触发间隔小于实际耗时，在单并发和最小间隔约束下并不能每 5 秒成功一次。 |
| 超时 | 180 秒 | 现场 60 秒超时造成第 2、4 次 Checkpoint 失败；给繁忙的 B 单并行度窗口和 JDBC 批量提交留时间。继续失败必须调查算子，不能无限增大。 |
| 最小间隔 | 2 秒 | 上次 Checkpoint 结束后至少空出 2 秒，避免快照紧挨着触发。 |
| 并发数 | 1 | 避免同一份窗口状态和多个 JDBC Sink 同时参与两次 Checkpoint，限制本机内存和 IO 压力。 |
| 失败容忍次数 | 3 | 短暂的慢快照不立即终止作业；连续失败超过 3 次仍必须暴露问题，不能靠容忍次数掩盖故障。 |
| 取消后保留 | `RETAIN_ON_CANCELLATION` | 保留已完成的外置快照用于人工检查；取消作业不会自动用旧快照恢复新作业。 |

本地目录是 Java 进程的 `user.dir` 下的 `checkpoints/`，IDEA 的工作目录可在运行配置里查看；Web UI 的 `Checkpoints -> Configuration` 必须显示 `FileSystemCheckpointStorage`。更换到固定盘符时可在 IDEA 运行配置设置环境变量，例如 `HOTNEWS_CHECKPOINT_DIR=file:///D:/study/hotnews-checkpoints`，先确保目录可写且剩余空间足够。Docker 运行时环境变量已有值，继续使用 Compose 的挂载路径。修改参数或环境变量只对重新启动的作业生效，当前运行的 Job 不会热更新。

## 本地失败记录（2026-10-07）

- 统一作业 Job ID `dd339b3fea40be10c9c75257286f08c5`，IDEA Web UI `localhost:10011`，观察时状态 `RUNNING`，共 42 个子任务。
- Web UI 配置：`JobManagerCheckpointStorage`、5 秒间隔、60 秒超时、最大并发 1、容忍失败 3 次。这证明 Checkpoint **已经启用**，但本地未配置明确的文件系统存储目录。
- Checkpoint 1 成功，42/42 子任务确认，耗时 24,331ms，状态大小约 5.3MB；所以“没有目录必然导致每次快照失败”不成立。
- Checkpoint 2 失败：`Checkpoint expired before completing`，60,018ms 时只有 28/42 子任务确认；Checkpoint 3 在约 49.6 秒标记 `Asynchronous task checkpoint failed`，23/42 确认；Checkpoint 4 也在约 60 秒超时，28/42 确认。
- Checkpoint 2 未确认的包含 `TumblingEventTimeWindows -> role_b_result -> MySQL category ranks`（0/1）、B 的迟到旁路（0/3）、B 的 Redis 快照下游（0/3）、Redis Sink（0/1）和打印算子。B 的单并行度窗口 Web UI 采样 `busyRatio=0.994`，`backpressureLevel=ok`：该窗口忙于计算，不可简单归咎于 Redis/MySQL Sink 的反压。Checkpoint 3 的异步失败底层 `Caused by` 在所提供的主线程异常与当前保存的 `role-all.log` 中均未出现，**不能断言具体是磁盘、序列化还是资源问题**。
- 旧异常 `Exceeded checkpoint tolerable failure threshold` 表示连续失败超限；`Recovery is suppressed ... maxNumberRestartAttempts=3` 表示随后重启次数耗尽。两者均是后果，不是最初失败原因。

## 再次运行记录（2026-10-07）

- IDEA Web UI：`http://localhost:10011`；统一作业 Job ID `ace6a97f964fefbc907f421ce4c3dd7e`。查询时作业 `RUNNING`，42 个子任务参与 Checkpoint。
- Web UI `Checkpoints -> Configuration`：`FileSystemCheckpointStorage`、`HashMapStateBackend`、`EXACTLY_ONCE`；间隔 30,000ms、超时 180,000ms、最小间隔 2,000ms、最大并发 1、容忍失败 3 次；取消时不删除外置快照。配置已从上次的 `JobManagerCheckpointStorage` 和 60 秒超时切换成功。
- 查询时累计 6 次完成、0 次失败、0 次恢复，均有 42/42 子任务确认。Checkpoint #1 至 #4 耗时分别为 51,910ms、43,216ms、63,593ms、49,567ms；#5、#6 分别为 55ms、87ms。后两次明显更快，仅记录观测值，不据此推断持续高负载时也能这么快完成。
- 已完成快照的外置路径示例：`file:/D:/study/study_flink/hotnews-realtime/checkpoints/ace6a97f964fefbc907f421ce4c3dd7e/chk-6`。查询时旧的 #1 至 #5 在 API 中已标记 `discarded: true`，因此“取消时保留”不代表每一份历史快照都会永久保留；恢复前应核实目标路径仍存在。
- B 的单并行度 `TumblingEventTimeWindows -> throughput: role_b_result -> Sink: MySQL category ranks` 曾采样 `busyRatio=1.0`、`backpressureLevel=ok`。说明该链路当时繁忙，不能只凭 busy 指标认定 MySQL/Redis Sink 是瓶颈。前 6 次成功说明此前的连续超时在本次观测期内没有复现，但还不能证明长时间稳定或故障后正确恢复。

## 第三次运行记录（2026-10-07）

- IDEA Web UI `http://localhost:10011`，新 Job ID `f3c4d030dae6400a03ac4bbab7854beb`；观察时统一作业 `RUNNING`，42 个子任务参与 Checkpoint。本次为新作业，没有从旧快照恢复（`restored: 0`）。
- Web UI 配置保持 `FileSystemCheckpointStorage`、`EXACTLY_ONCE`，间隔 30,000ms、超时 180,000ms、最小间隔 2,000ms、并发 1、容忍失败 3 次；取消时保留外置快照。
- 截至本次查询，Checkpoint #1、#2、#3 均成功（各 42/42 确认），分别耗时 41,576ms、61,376ms、55,151ms，状态大小分别为 12,216,536、7,243,030、8,354,535 字节；累计完成 3 次、失败 0 次。查询时 #4 刚触发、仍为 `IN_PROGRESS`，不计入成功次数。
- #3 的外置路径为 `file:/D:/study/study_flink/hotnews-realtime/checkpoints/f3c4d030dae6400a03ac4bbab7854beb/chk-3`。此前 #1、#2 已标记 `discarded: true`；不能将历史外置路径等同于一直可用的恢复点。
- Rule B 的单并行度 `TumblingEventTimeWindows -> throughput: role_b_result -> Sink: MySQL category ranks` 当时为 `RUNNING`；本次没有采到可信的 busy/backpressure 数值，因此不把上次的采样当作这次的指标。三次成功只证明本次观察期内未复现连续 Checkpoint 失败；长期稳定性和从快照恢复后结果正确性仍未验收。

## 下一次运行的核对方式

1. 停止旧作业后重新编译并启动统一作业，确认 Web UI `Checkpoints -> Configuration` 显示文件系统存储、30 秒间隔、180 秒超时；不要删除 Kafka Topic、MySQL 结果或旧 Checkpoint。
2. 在 `Checkpoints -> History` 查看成功次数、持续时间与失败消息，点击失败编号查看 `Task` / `Subtask`；尤其关注 B 的窗口算子。若仍出现异步失败，保存对应 TaskManager/IDEA 日志中 **第一条** `Caused by`，不能只保存主线程末尾异常。
3. 记录 B 窗口的 `busy`、`backpressure` 和 Checkpoint `start delay`、`alignment`、`sync`、`async` 时间。若一直高 busy 且不确认 Barrier，需针对 B 的 `windowAll` 单并行度计算量处理，继续调大超时并不能保证成功。
4. 核对已完成快照的 External Path 是否可访问；若是 `<checkpoint-not-externally-addressable>`，检查环境变量、工作目录和是否运行的是新编译代码。不要把目录存在或作业 `RUNNING` 当成恢复验收。

## 一致性边界

Flink Checkpoint 保存算子状态和 Kafka 消费进度；`EXACTLY_ONCE` 指 Flink 内部快照与 Kafka 位点的一致恢复。MySQL JDBC Sink 在 `snapshotState` 前提交批次，但与 Checkpoint 不在同一个分布式事务；失败后可能重放已提交记录，依靠业务唯一键和 UPSERT 收敛。Redis 最新榜单使用窗口起点与修订号比较及 2 小时 TTL，也不参与 Flink 事务；Key 过期后失去旧版本记忆。因此不能宣称 MySQL/Redis 与 Flink 端到端原子 Exactly-Once。恢复后应按同一批 JSONL 的独立 SQL、MySQL 主键结果和 Redis 最新窗口分别核对。

## 第五天 Docker RocksDB 基线（2026-10-08）

### 概念说明

运行中成功快照证明状态和位点可持久化；输入完成后的成功快照不能单独证明失败时能重放记录。自动恢复、人工 Savepoint 恢复、旧 Checkpoint 重放和外部结果正确性要分别取证。

### 项目中的具体实现

本轮 Job ID `d933102253fcaaeb92dd34ce2faa8750`，Docker Web UI 8081，用户确认 MySQL/Redis 已清空。实际配置为 RocksDB、文件系统存储、30s 间隔、180s 超时、2s 最小间隔、并发 1、容忍 3、取消保留、对齐快照。配置来自 `/jobs/{jobId}/checkpoints/config`，不是由 Compose 文件推断。

### 测试结果

12:39:52 附近采集：成功 10、失败 0、restored=0。负载阶段 #1~#8 耗时 19.982~92.425s；空闲阶段 #9/#10 为 477/323ms。#10 状态 11,000,675 bytes，实际本次 checkpointed_size 为 3,594,431 bytes。最新 External Path 为 `file:/opt/flink/checkpoints/day5/d933102253fcaaeb92dd34ce2faa8750/chk-10`。

MySQL 两次查询一致：clean=95082、A=25、B=55、C=1；Join late=61280，A/B/C late=52686/54945/57382。Kafka 当前作业两消费组各分区 committed_lag=0。恢复前告警逐字段及 Redis Hash/TTL 已保存到 `tests/roles/recovery/evidence/`。具体编号、字段和时序图见 `06-故障演练与恢复SOP.md`。

### 问题

SQL 基准 A=123/B=64/C=1，本轮结果尚不一致。#1~#7 已被清理，#8~#10 在采集时仍 retained，目录不能当成永久恢复点。尚未执行 Kill、Savepoint 或旧 CK 重放；API 文件归档已完成，浏览器截图尚未保存。

### 结论

本轮观察期 Checkpoint 成功，不能据此写“第五天故障恢复通过”。后续必须记录 restored 指向、TM 故障/重新注册时刻、重放位点及 MySQL/Redis 内容变化，并独立报告 SQL 正确性差异。

### 首次 Kill 实操结果与处理

北京时间12:43:51.156检测到TM不可达，12:43:56/12:44:01/12:44:06三次从#17尝试恢复，均遇到 `NoResourceAvailableException`；12:44:06.460作业FAILED，12:44:06.756新TM才注册8个slot，差296ms。JM存活，但5秒重试间隔短于本次约15.6秒的TM注册过程。完整JM日志/异常在 `tests/roles/recovery/evidence/20261008-124454-500-failed-after-kill`，不能只依据restored=3标记恢复通过。

CK累计成功17、失败1；#18因触发失败，#15/#16/#17保留。数据库8张取证表行数不变、A/B/C导出SHA256相同，Redisranking/window/revision未变、TTL自然下降；未执行任务重放，不能据此证明幂等性。

已将统一入口的恢复策略覆盖为3次、间隔30秒，Checkpoint和业务状态结构不变；12:46编译打包成功，未运行额外测试。下一步从保留的#17人工提交新JAR恢复现场，再复测自动恢复。本次失败归因是资源注册与重试时机，不是已经证实的存储目录缺失。步骤、时间线及新JAR摘要见 `06-故障演练与恢复SOP.md`。

随后人工恢复提交遇到独立问题：客户端尝试 `jobmanager:10011` 而集群监听8081，提交前就连接失败，没有进入#17快照加载。根因是统一入口无条件传入本地Web UI端口。已限制为只有本地 `LocalStreamEnvironment` 设置10011，集群沿用配置；12:52:22重新打包成功，跳过测试，尚待再次提交验证。该问题与状态损坏、Checkpoint清理或TM重试是不同阶段，详细异常和最新JAR摘要记在SOP。

### 从保留的 #17 人工恢复结果

12:54提交的新Job `29b706b9d0643dc1352dcc6cd2aee6bf` 已42/42子任务RUNNING，实际重试间隔30000ms，RocksDB及原Checkpoint配置生效；restored=1，路径准确指向旧Job的 `chk-17`。12:55快照确认新#18/#19成功、失败0。REST时长430/200ms，JM日志2904/2122ms，两种接口数值分别记录，不混用；时间差异原因尚未确认。

MySQL8张表行数不变，A/B/C全部导出SHA256与故障前相同；Redis窗口、revision、完整ranking不变、TTL正常下降。新Source输出0，说明从输入末尾恢复，不能凭相同结果证明实际重放幂等。恢复元数据把CLI导入#17标记is_savepoint=true，不等于本次主动创建了Savepoint。完整证据目录 `tests/roles/recovery/evidence/20261008-125500-171-restored-from-ck17`。

结论：原RocksDB状态可被配置修正后的JAR加载，人工恢复通过；第一次Kill自动恢复仍失败，需第二次验证；Savepoint创建/配置修改恢复、实际重放幂等及SQL全量正确性尚未通过。

### 第二次 Kill 自动恢复记录

#### 概念说明和项目实现

自动恢复保持同Job，不通过CLI重提交。Job层RUNNING、restored计数、全部子任务RUNNING及新的成功CK联合核对；配置为3次重试、间隔30秒、RocksDB，业务状态和Watermark沿用基线。

#### 测试结果

Job `29b706b9d0643dc1352dcc6cd2aee6bf`在12:57:18.412被JM确认旧TM不可达，12:57:18.415进入RESTARTING，12:57:48.421从同Job #23恢复；12:57:50.549全部42任务运行，故障检测至任务全部恢复约32.137s。新TM注册时间12:57:10.380，本轮注册早于JM故障检测。Job ID不变，无人工提交。

恢复后#24于12:58:20.584完成，42/42确认，累计成功7/失败0/restored2；REST时长231ms、JM日志2476ms分别归档。MySQL8张表行数不变，A/B/C导出SHA256完全相同；Redis排名、窗口、revision一致，TTL自然减少。证据目录 `tests/roles/recovery/evidence/20261008-125812-688-second-kill-restored`，初期观察及时间线详见SOP。

#### 问题和结论

这次已验证空闲阶段单TM故障的自动状态恢复及后续成功Checkpoint。尚无记录重放，不能标记Sink重复写入验收；两次TM注册时序不同，也不把恢复成功完全当成修改等待参数的受控因果实验。正在处理输入时Kill、Savepoint创建和配置恢复、SQL全量一致性均仍待完成。

### Savepoint 创建与非状态配置准备

用户主动创建Savepoint成功，REST确认#30/SAVEPOINT/CANONICAL、42/42确认，External Path为 `file:/opt/flink/savepoints/savepoint-29b706-b070e19bc66a`。北京时间13:00:55.402触发、JM13:00:58.072完成；REST556ms、JM2670ms，状态23,531,049 bytes。挂载_metadata已存在，详尽证据归档 `tests/roles/recovery/evidence/20261008-130216-099-savepoint30-before-config-change`。

此时Job继续运行，外部结果与之前数量一致。已编辑 `.env` 将JDBC批量200改100；运行中容器尚保留200。该参数在Sink open读取，与Keyed State描述符/业务拓扑无关，需取消作业、仅重建JM/TM、确认新环境100，再从本Savepoint提交。创建成功不等于配置修改恢复通过；重建前历史数据已保存，后续不依赖旧Session UI保留历史。操作命令见SOP。

### Savepoint #30 恢复和批量配置修改结果

新Job `78319e421e7d0f398bf2dffbf9ec6852` 从 `savepoint-29b706-b070e19bc66a` 恢复，REST restored=1、is_savepoint=true。启动期间9个任务尚在初始化，05:05:30/05:06:00两次CK因任务未全部RUNNING而失败；05:06:29.684最后任务运行，之后#31/#32均42/42成功，JM checkpointDuration 4442/4131ms。状态兼容性通过，不能把启动期失败写成快照不兼容。

`.env`已从批量200改为100并随重建后的作业配置使用；没有新增输入，MySQL A/B/C完整导出、Redis ranking/window/revision均不变，TTL自然减少。结果保持只能证明恢复后没有改变已写结果，不能证明实际记录重放时Sink幂等；批量100的吞吐影响也没有测量。完整证据目录 `tests/roles/recovery/evidence/20261008-130733-364-savepoint30-restored-batch100`。

结论：主动Savepoint创建、修改非状态配置后状态恢复及后续Checkpoint已通过；状态核心结构没有修改。启动期两次CK失败为任务初始化时机问题。实际重放幂等、处理输入中Kill和SQL全量一致性仍是未完成项。

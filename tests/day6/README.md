# 第六天：隔离反压压测

本目录只创建 `hotnews-day6-*` Kafka Topic 和消费组；模拟 Sink 不写 MySQL/Redis，
不修改 `Achieve_roleA.java`、现有生产 Topic、业务表或数据卷。Docker Compose
沿用 `deploy/docker-compose.yml` 和外置 `deploy/.env`；共享集群已有运行中作业时，
脚本拒绝提交，不会替别人停作业。需要已启动 Kafka、Flink 和 TaskManager。

## 类的职责与思路

| 类 | 职责及实现思路 |
| --- | --- |
| `Day6BackpressureJob` | 从规则 A 复制五分钟滑窗状态与清理逻辑；从独立 Source 分流到窗口规则和慢 Sink，关闭慢 Sink 的链式合并以观察网络反压。 |
| `Day6SyntheticSource` | 单调时钟限速，按 Source 并行度分担总速率；默认 200 键，记录源端发出时间。 |
| `Day6DelaySink` | 每条记录等待指定毫秒，并登记消费量和最近一条的延迟；`mysql`/`redis` 只标识模拟目标，不是实际 I/O。 |
| `Day6KafkaRecordDeserializer` | 从独立 Kafka 性能 Topic 的分区和 offset 生成事件 ID，保留 Kafka 消息时间戳计算真实排队时延。 |
| `Day6BackpressureJobTest` | 拒绝无界参数、过高负载和非隔离 Topic/消费组。 |

## 复现

在项目根目录执行；**先确认共享集群空闲**：

```powershell
mvn -o -f flink-job/pom.xml -pl flink-rolesachieve -am test
mvn -o -f flink-job/pom.xml -pl flink-rolesachieve -am -DskipTests package
& tests/day6/run.ps1 -Stage stable-2000 -Rate 2000 -Seconds 35
& tests/day6/run.ps1 -Stage stable-5000 -Rate 5000 -Seconds 35
& tests/day6/run.ps1 -Stage slow-p2 -Rate 2000 -Seconds 25 -DelayMs 2 -SinkParallelism 2
& tests/day6/run.ps1 -Stage tuned-p4 -Rate 2000 -Seconds 25 -DelayMs 2 -SinkParallelism 4
& tests/day6/run.ps1 -Stage state-growth -Rate 2000 -Seconds 25 -Keys 10000
& tests/day6/run.ps1 -Stage rule-output -Rate 2000 -Seconds 20 -Keys 10
```

真实 Kafka Lag 使用独立短保留 Topic。首条命令自动创建唯一 Topic（保留 1 小时）、
以指定速率发送 200 字节记录、采样、最后仅取消自身 Job；从其输出读取 Topic
与消费组，再执行第二条。追平作业沿用消费组已提交位点，连续两次 Lag=0 且
Checkpoint 成功才通过。

```powershell
node tests/day6/run-kafka.mjs slow 2000 20 2 2
node tests/day6/recover-kafka.mjs <上条输出的Topic> <上条输出的消费组> 4
node tests/day6/run-kafka.mjs fast5000 5000 20 0 2
```

`capture.mjs` 单独运行形式：
`node tests/day6/capture.mjs <32位JobID> <标签> 30`；
设置 `DAY6_KAFKA_GROUP` 后还读取 Kafka 消费组 Lag。它将算子指标、
Checkpoint 和 TaskManager 内存/GC/网络缓冲快照保存到 `results/`，
新文件不覆盖旧文件。失败作业应到 Flink REST 的 `/jobs/<id>/exceptions`
检查，不能把作业显示为 RUNNING 就当作吞吐通过。

## 证据边界

- `numRecordsIn/Out` 是 Flink 计数；作业结束后的算子总数用于最终核对。
  REST 自定义逐子任务计数可能约 10 秒才刷新一次，不能据短间隔空值
  判定吞吐为零。每档输入数应等于规则输入与模拟 Sink 输入数。
- `day6_last_latency_ms` 是某个 Sink 子任务最近一条记录的延迟，
  不是 p95；Kafka 模式以 Kafka 记录时间戳计算，包含 Kafka 排队。
- 合成 Source 的内部积压不是 Kafka Lag；只有 Kafka 模式的
  `kafkaLag.rows[].lag` 才是消费组提交位点到 Log End Offset 的差，
  Checkpoint 提交周期会造成台阶。模拟 Sink 延迟不代表真实 MySQL/Redis
  的事务、网络、批量写入与重试能力。
- 结束时的 Checkpoint 可能只剩已清理状态；状态规模比较应看运行中
  `samples[].checkpoints.latest.completed.state_size`。
- 不做真实 OOM 注入、不停服务、不删 Topic/数据卷；短保留期 Topic
  的清理由 Kafka 自身按配置执行。复现实验前先评估宿主机容量。

指标、现象定位、容量测算和三项代码审计见
`docs/05-反压压测与容量规划.md`。

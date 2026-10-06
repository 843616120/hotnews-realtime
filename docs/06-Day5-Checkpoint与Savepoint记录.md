# Day 5：Checkpoint/Savepoint 配置与恢复记录

本页区分 **2026-10-06 已有现场证据** 和 **尚需现场核验**。
复现实操见 [操作手册](04-Day5-Sink与恢复验收.md)；原始 JSON 在
[`tests/day5/evidence/`](../tests/day5/evidence)，背景见
[`tests/day5/README.md`](../tests/day5/README.md)。
文中历史 Job ID/目录**不能**当作此刻仍可恢复的路径。

## 配置、位置与依据

| 项目 | 本项目设置 | 配置来源与取舍 |
| --- | --- | --- |
| 存储目录 | `file:///opt/flink/checkpoints/day5`；Savepoint `file:///opt/flink/savepoints` | JobManager/TaskManager 共用宿主机 `D:\docker_data\flink\checkpoints`、`D:\docker_data\flink\savepoints` 绑定挂载。`deploy/flink-conf.yaml` 的 `state.checkpoints.dir` 是父目录；作业里 `HOTNEWS_CHECKPOINT_DIR` 环境变量覆盖成 `/day5`，避免与其他作业混放。跨容器必须是同一路径且持久化，`file:` 在单机本地训练可用，跨宿主机集群应换成共享持久存储。 |
| 模式 | `EXACTLY_ONCE` | Flink 内部算子状态和 Kafka 消费位点一起快照；**不代表** MySQL/Redis 参与原子提交。 |
| 间隔 | `10s` | 演练环境以短恢复点间隔验证故障，最多约一个间隔的 Source 进度需要重放；用实际成功率、I/O/反压再调整，并非端到端 RPO 保证。 |
| 超时 | `60s` | 为 200 行 JDBC 批量 flush、Barrier 对齐及约 54 MB 状态快照留余量；实际快照多为亚秒级，但 Windows 挂载和压力可能抖动。超时先查失败原因，再调大。 |
| 最小暂停 | `2s` | 让一次快照结束后 MySQL/状态后端有喘息时间，避免持续 Checkpoint 压力。 |
| 最大并发 | `1` | 避免多个快照同时刷写 JDBC、争用共享目录，便于时序与故障定位。 |
| 可容忍失败 | `3` | `Day5SinkJob` 允许至多 3 次可计入的连续 Checkpoint 失败，偶发超时不立即使作业失败；持续失败仍应告警。恢复过程中触发失败累计数不能直接等同“连续超过 3 次”。 |
| 重启 | 固定延迟 `5s`、最多 `3` 次 | 单 TaskManager 演练允许短暂故障自动重启；超过预算仍需人工查资源、日志和失败 Checkpoint。 |
| 保留 | 取消作业时 `RETAIN_ON_CANCELLATION`；配置保留最近 `3` 个 | 可用外部 Checkpoint 手动恢复，但老 `chk-N` 会清理；Savepoint 单独保存以供有计划恢复。手动恢复前检查 `_metadata` 实际存在。 |

配置源：
[`Day5SinkJob.java`](../flink-job/flink-rolesachieve/src/main/java/Day5SinkJob.java)
设定 interval、timeout、min-pause、并发、容忍次数、保留和重启策略；
[`deploy/flink-conf.yaml`](../deploy/flink-conf.yaml)
设定集群默认值及目录；
[`deploy/docker-compose.yml`](../deploy/docker-compose.yml)
提供容器共享挂载和 `HOTNEWS_CHECKPOINT_DIR`。单独配置
`state.backend.rocksdb.localdir` **不代表**作业已经启用 RocksDB；
当前这次演练有 `HeapRestoreOperation` 的恢复栈证据，不能写成
“RocksDB 恢复耗时”。变更挂载/目录后需要重新创建受影响的 Flink
容器，并先确认现存 Savepoint 可达；文档改动本身不修改运行时配置。

## 历史现场记录

记录日期：**2026-10-06（上海时区）**；JSON 的 `capturedAt` 是 UTC
（开始采样于 2026-10-05 18:09Z）。固定 Topic 当时包含文章 500 条、
行为 100000 条原始数据，非本次编辑后的新验收。

| 阶段 / 证据 | Job 与 Checkpoint | 外部结果与解释 |
| --- | --- | --- |
| [故障前](../tests/day5/evidence/before-kill.json) | `f43a63bf5c657db15de246feda4c1cb2`，成功 `chk-16`，734 ms、约 53.8 MB；已成功 16 次 | 明细 97044、A 105、B 25、C 活跃 0、异常 64824；Redis 窗口 `1790469600000`、revision `15004`、Top 5。 |
| [恢复中](../tests/day5/evidence/during-recovery.json) | TaskManager SIGKILL 后恢复来源 `chk-19`；此时已有 19 次成功，恢复期快照触发失败 | Job 顶层 `RUNNING` 还不能判定全部算子恢复。状态恢复涉及 C 算子约 42.3 MB；单 TaskManager 资源紧张。 |
| [故障后](../tests/day5/evidence/after-kill.json) | 11/11 顶点运行；新成功 `chk-31`，807 ms | 明细/A/B/C 分别 97044/105/25/0；A/B 按业务键核对未倒退/累加，Redis Top 5 与窗口/revision 未倒退；异常增加 18 条。 |
| [Savepoint 前](../tests/day5/evidence/before-savepoint-restore.json) | 原 Job 成功 `chk-41`，722 ms；统计累计 226 次失败 | 异常表 64842；不把“226 次累计失败”写成“连续超过容忍 3 次”。 |
| [Savepoint 后](../tests/day5/evidence/after-savepoint-restore.json) | Savepoint `file:/opt/flink/savepoints/savepoint-f43a63-bae3b1f5f00c`；取消原 Job 后，新 Job `995346f409afd03a37db6459a5476c0d`，11/11 顶点运行、最新成功 `chk-72`，721 ms | 以 `--mysql-batch-size 100` 恢复；Redis 窗口、revision、Top 5 和剔除 `detect_time` 的语义哈希与恢复前一致。**MySQL 恢复后逐键查询未取得**。 |

真实 Savepoint 首次恢复曾遇到 `No suitable driver found`，失败 Job
`9b1bf3a99a2701ca890e333be24dd0e3` 已取消；在 JDBC Sink 初始化中
显式 `Class.forName("com.mysql.cj.jdbc.Driver")` 后重新构建，第二次
才得到上表的成功 Checkpoint。这是代码初始化修复，不是演练从头到尾
一次成功的证据。详细故障上下文和探针见
[`tests/day5/README.md`](../tests/day5/README.md)。

### 逐项验收判定

- **已核对**：Kill 后从 `chk-19` 自动恢复，新成功快照 `chk-31`；
  Kill 前后 MySQL 清洗/A/B 行数及 A/B 业务键、Redis 版本与完整榜单。
  MySQL Sink 已有主键批量 UPSERT，Redis 独立 Key 的旧窗口/旧修订号
  Lua 探针通过（临时 Key，不是旧 Checkpoint 全链路回放）。
- **已核对状态加载**：Savepoint 恢复后 11/11 算子、新成功
  `chk-72` 和 Redis 语义哈希；该新 Job 的 JSON 中累计失败 17 次，
  不能把“最后成功”写成“恢复全过程没有失败”。
- **本轮复验（2026-10-06 13:26，上海）**：
  `HOTNEWS_LIVE_REDIS_TEST=1` 的独立 Key 探针通过（2 项中
  Redis 1 项通过、MySQL 1 项因未启用跳过）；全量 Maven
  `test` 共 24 项、22 通过、2 个现场探针未启用而跳过。
  Kill 前后旧证据用 `tests/day5/compare.mjs` 复核：
  A 的 105 个键、B 的 25 个键检查均无差异。
- **尚未核对**：Savepoint 恢复后的 MySQL 逐键结果；旧
  Checkpoint 回放对业务表的完整对比；MySQL 临时表 UPSERT 现场探针；
  常驻流停写后的有界最终 A=123/B=60。此前宿主机用现有凭据
  MySQL 登录被拒，不能绕过认证或把 REST `RUNNING` 当 SQL 证据。
- **边界**：C 无单调修订号，旧快照反向回放时可能覆盖更晚的撤销；
  Redis Key 过期后也无跨过期的单调版本保护。活跃 C 告警和
  TTL 过期场景不在“所有外部结果不会倒退”的已通过范围内。

## 补验记录模板

在获得授权且确实执行后再填；每次使用新 stage 名，不覆盖旧 JSON。
采样工具与对比方法见[操作手册第 7 节](04-Day5-Sink与恢复验收.md)。

| 项目 | 待填写的现场值 |
| --- | --- |
| 执行日期、原 Job ID、新 Job ID、JAR 版本 | 待测 |
| 快照类型、真实 `external_path`、`_metadata` 检查 | 待测 |
| 恢复前成功 ID/次数/时长、恢复后新成功 ID/次数/时长 | 待测 |
| MySQL 前后五表行数、A/B 业务键比较和 C 撤销 | 待测 |
| Redis 窗口/revision、Top 5、语义哈希、TTL | 待测 |
| 故障日志、重放失败原因和结论 | 待测 |

仅在常驻 Day 5 作业停写后才启动隔离有界模式；Kafka 历史事件可导致
外部表再次收到写入，核验必须在新作业追平后进行。若需长时间保留
某个恢复点，应显式创建 Savepoint 并妥善保存；不能依赖仅保留
3 个的历史 Checkpoint 目录。

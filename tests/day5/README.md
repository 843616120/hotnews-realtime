# Day 5 现场验收

## 输入与隔离

- 2026-10-06（Asia/Shanghai）：Kafka `topic_article` 三个分区末尾位点
  `172+160+168=500`；`topic_behavior` 六个分区
  `4788+5208+4968+40405+39371+5260=100000`。
- MySQL 五张目标表启动时已存在且 `clean_behavior=0`；
  Redis `hotnews:top5:latest` 初始为空。未清空 Topic 或数据卷。
- 作业 `f43a63bf5c657db15de246feda4c1cb2` 通过 Flink REST
  上传并运行已测试打包的 `Day5SinkJob`（首轮 21 项 Java 测试通过）。
  常驻流不强行注入未来水位线，末端未关窗的 10 分钟结果
  不应直接与固定批次的 A=123、B=60 比较。

## 实际故障注入

1. `before-kill.json`：作业运行中，Checkpoint 16 成功（734 ms），
   已记录 MySQL 各表行及排序后原始 A/B 行、Redis Top 5 哈希与 TTL。
2. 对本 Compose 的 TaskManager 执行 SIGKILL，再启动服务。
   Flink REST 报告从 `chk-19` 恢复；`during-recovery.json`
   记录了期间的 Checkpoint 失败和外部存储不回退情况。
3. `chk-19` 快照约 53.8 MB，其中规则 C keyed state 约 42.3 MB；
   单 TaskManager 加 Windows 绑定挂载恢复耗时较长。线程栈显示规则 C
   在 `HeapRestoreOperation` 读取并反序列化状态；恢复期间累计
   226 次 Checkpoint 触发失败，但之后 11/11 算子运行并产生
   新的成功 Checkpoint 31。恢复期顶层作业 `RUNNING` 不等于恢复完成。
4. 同期集群另有两条 `ArticleHeatStateJob` 被提交并持续重启；
   本验收没有停止或修改它们。它们可能影响共享 TaskManager 的容量。
5. `after-kill.json` 与 `before-kill.json` 逐键比对通过：
   MySQL 明细均为 97044 行，A 105 行、B 25 行没有倒退或累加；
   Redis 均为 5 条榜单，窗口 `1790469600000`，修订号 15004。
   异常旁路增加 18 行，此处不将异常落库视为完成补算。

## Savepoint 恢复

- 从作业 `f43a63bf5c657db15de246feda4c1cb2` 创建 Savepoint：
  `file:/opt/flink/savepoints/savepoint-f43a63-bae3b1f5f00c`。
  已在宿主机共享 Savepoint 目录确认存在。取消原作业后，使用相同 JAR、
  相同消费组与 `--mysql-batch-size 100` 提交。
- 第一次提交的作业 `9b1bf3a99a2701ca890e333be24dd0e3` 暴露
  Flink 类加载器下 JDBC 驱动未自动注册（`No suitable driver found`）。
  已取消失败作业，显式加载 `com.mysql.cj.jdbc.Driver` 后重新打包；
  Java 测试 22 项通过。此为非状态 Sink 初始化修复，不变更算子状态拓扑。
- 修复后的作业 `995346f409afd03a37db6459a5476c0d` 从同一
  Savepoint 恢复；`after-savepoint-restore.json` 记录 11/11
  算子 `RUNNING`、36 次成功 Checkpoint（最新 ID 72、721 ms）。
  Redis 窗口、修订号、5 条榜单和排除 `detect_time` 的语义哈希
  与恢复前一致。MySQL 恢复后逐键查询**尚未验证**：宿主机
  使用 `deploy/.env` 口令登录被拒绝（可能是凭据或主机授权问题），
  不能以 REST 状态代替 SQL 证据。

## 快照与重试

在项目根目录：

```powershell
node tests/day5/docker-engine.mjs ps
node tests/day5/capture.mjs before-savepoint <Day5-Job-ID>
```

`capture.mjs` 只读 Flink REST、MySQL 和 Redis，将作业状态、
Checkpoint 成功/失败/恢复次数、MySQL 完整的 A/B 业务键和计数、
Redis 最新窗口/修订号/排名 SHA-256/TTL 保存至 `evidence/<stage>.json`。
后续运行 `capture.mjs` 须由用户显式提供有效 `MYSQL_PASSWORD`；
无凭据时用 `capture-runtime.mjs` 只采 Flink/Redis。
快照文件不覆盖已存在的同名证据。Docker Engine 命名管道访问需要
当前用户有相应权限；有 Docker CLI 时也可用 `docker compose exec`。

独立幂等探针 `Day5LiveSinkIntegrationTest` 使用 MySQL 临时表和
Redis 独立 Key，不修改生产业务行；分别用
`HOTNEWS_LIVE_MYSQL_TEST=1`（需用户提供有效 `MYSQL_PASSWORD`）、
`HOTNEWS_LIVE_REDIS_TEST=1` 启用。宿主机无法登录运行实例时，
MySQL 探针应标为未验证，不从容器提取凭据。

Redis 实际 Lua 临时 Key 幂等探针通过：旧窗口、旧修订号被拒绝，
较新修订号可覆盖，TTL 在 0..7200 秒内。Java 现场测试共 24 项：
23 通过、MySQL 临时表测试因宿主机凭据/授权未确认而跳过。
曾用 `deploy/.env` 口令运行一次 MySQL 探针，报告 `Access denied`，
没有改变业务行；
没有从容器读取或导出口令。提供有效 `MYSQL_PASSWORD` 后才可开启
`HOTNEWS_LIVE_MYSQL_TEST=1` 并继续核验。

最终验收还需完成 MySQL Savepoint 恢复后逐键查询，
并在常驻作业停写后用 `--bounded` 固定批次核对 A=123、B=60。
隔离有界回放仅在常驻作业停写之后运行；如果无法完成，
保留原始失败指标，不将待验证项目标记为通过。

获得用户明确提供的宿主机可用 MySQL 连接配置后，在同一终端设置
`MYSQL_USER`、`MYSQL_PASSWORD`、`MYSQL_DATABASE`（密码勿写入 Git），
依次执行：

```powershell
$env:HOTNEWS_LIVE_MYSQL_TEST = '1'
mvn -o -f flink-job/pom.xml -pl flink-rolesachieve -am test
node tests/day5/capture.mjs after-savepoint-mysql 995346f409afd03a37db6459a5476c0d
node tests/day5/compare.mjs tests/day5/evidence/before-savepoint-restore.json tests/day5/evidence/after-savepoint-mysql.json
```

然后在无并发写入的维护时段停止常驻作业，运行
`Day5SinkJob --bounded --mysql-batch-size 100` 读取同一固定批次；
用 `sql/queries/day5-check.sql` 核验 MySQL，并核对 Redis 最新窗口、
修订号和 TTL。不得为了让终端水位线前进而向生产 Topic 写入虚假未来事件。

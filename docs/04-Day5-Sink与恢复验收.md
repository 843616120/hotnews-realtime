# Day 5 操作手册：提交、查询与状态恢复

在项目根目录的 **PowerShell** 执行以下命令。默认六个 Compose 服务已启动；
Flink Web UI 为 `http://localhost:8081`。先阅读
[参数与演练记录](06-Day5-Checkpoint与Savepoint记录.md) 和
[一致性边界](05-Day5-一致性边界.md)。
以下 `<...>` 代表现场实际值，不要把尖括号原样输入。勿提交真实数据库口令。

## 1. 准备 JAR 与数据库

```powershell
$compose = @('--env-file', 'deploy/.env', '-f', 'deploy/docker-compose.yml')
docker compose @compose ps
mvn -o -f flink-job/pom.xml -pl flink-rolesachieve -am package
docker compose @compose config
docker compose @compose exec jobmanager ls -l /opt/flink/usrlib/flink-rolesachieve-1.0-SNAPSHOT-all.jar
```

若 Maven 离线依赖不全，去掉 `-o` 再构建。Compose 把
`flink-job/flink-rolesachieve/target` **只读**挂到 JobManager 的
`/opt/flink/usrlib`；重新打包后新提交的作业读新 JAR，
已运行的作业不会自动热更新。不要把构建目录之外的 JAR 路径直接传给容器。

首次创建 MySQL 数据卷时，`sql/01-day5-tables.sql` 会自动建表。
已有数据卷或要确认表结构时，进入交互式 MySQL（在密码提示处输入
**当前数据库实际口令**，不保证与事后修改的 `deploy/.env` 相同）：

```powershell
docker compose @compose exec mysql mysql -uroot -p hotnews
```

在 `mysql>` 中输入（仅缺表时执行 `SOURCE`，该脚本的 `IF NOT EXISTS`
不会修改既有表结构）：

```sql
SOURCE /docker-entrypoint-initdb.d/01-day5-tables.sql;
SHOW TABLES;
SHOW COLUMNS FROM category_rank LIKE 'revision';
-- 若旧表确实缺少 revision，先确认备份及现有数据，再单独执行：
-- ALTER TABLE category_rank ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;
```

若不能登录，先核实管理员提供的账户与权限；不要清空数据卷来重置口令。
建表/唯一键和查询原件分别见
[`sql/01-day5-tables.sql`](../sql/01-day5-tables.sql)、
[`sql/queries/day5-check.sql`](../sql/queries/day5-check.sql)。
交付的 Sink 实现：
[`Day5MySqlSink.java`](../flink-job/flink-rolesachieve/src/main/java/Day5MySqlSink.java)
与
[`Day5RedisRankSink.java`](../flink-job/flink-rolesachieve/src/main/java/Day5RedisRankSink.java)。
本项目固定输入已在 Kafka 时不要重新发送种子批次；新环境才按
[根 README](../readme.md) 创建 Topic 与生成数据。

## 2. 提交持续作业并确认 Checkpoint

先检查 Web UI 中没有其他 **RUNNING** 的 `Day5SinkJob`。不能将两个
同消费组的常驻/回放作业并行写入相同业务表和 Redis Key。

```powershell
docker compose @compose exec jobmanager `
  flink run -d -c Day5SinkJob /opt/flink/usrlib/flink-rolesachieve-1.0-SNAPSHOT-all.jar
docker compose @compose exec jobmanager flink list
```

记下返回的 **32 位 Job ID**，之后在当前 PowerShell 设置：

```powershell
$job = '<本次提交返回的 Job ID>'
$rest = 'http://localhost:8081'
(Invoke-RestMethod "$rest/jobs/$job").state
(Invoke-RestMethod "$rest/jobs/$job").vertices |
  Select-Object name,status
$cp = Invoke-RestMethod "$rest/jobs/$job/checkpoints"
$cp.counts
$cp.latest.completed | Select-Object id,status,end_to_end_duration,external_path
$cp.latest.restored
```

等待 `counts.completed` 增长、`latest.completed.status=COMPLETED`、
所有顶点 `RUNNING`；保存成功 ID、时长、存储路径和时间。
只有 `RUNNING` 不表示恢复成功；失败原因在 Web UI 的
Job -> Checkpoints 和 TaskManager 日志：

```powershell
docker compose @compose logs --tail=150 taskmanager
```

## 3. 核对 MySQL 与 Redis

进入 `mysql -uroot -p hotnews` 后执行：

```sql
SOURCE /docker-entrypoint-initdb.d/queries/day5-check.sql;
SELECT window_start_ms, article_id, click_count FROM article_alert
  ORDER BY window_start_ms, article_id LIMIT 10;
SELECT window_start_ms, rank_no, category, score, revision FROM category_rank
  ORDER BY window_start_ms DESC, rank_no LIMIT 10;
SELECT COUNT(*) FROM ip_alert WHERE retracted = 0;
```

MySQL 主键：明细 `event_id`，A `(window_start_ms,article_id)`，
B `(window_start_ms,rank_no)`，C `(alert_minute_ms,ip)`，
异常 `(event_type,event_id)`。A 写入取 `GREATEST` 而不是 `count+1`；
B 依 `revision` 拒绝旧版本。C 的撤销行保留主键，查询活跃值时过滤
`retracted=0`。异常表不是已完成补算的证明。

```powershell
docker compose @compose exec redis redis-cli TYPE hotnews:top5:latest
docker compose @compose exec redis redis-cli HGETALL hotnews:top5:latest
docker compose @compose exec redis redis-cli HGET hotnews:top5:latest ranking
docker compose @compose exec redis redis-cli TTL hotnews:top5:latest
```

Key 类型是 Hash，字段 `window_start_ms`、`window_end`、`revision`、
`ranking`（完整 Top 5 JSON），TTL 写入时为 7200 秒；每次被接受的
新写入会刷新 TTL。`TTL=-2` 表示 Key 不存在，过期后失去旧版本记忆。
记录窗口、修订号、JSON 条数和 TTL；在输入稳定、结果已追平时逐键比较，
不要把 TTL 递减或 `detect_time` 的变化当成业务数据错误。
常驻流尚未关闭的窗口不应与有界回放的最终值比较：
固定输入在有界终止水位线下的目标为明细 97044、A 123、B 60、C 活跃 0，
不是持续流任意时刻必须达到的行数。

## 4. Kill TaskManager，观察自动恢复

**仅在可接受中断共享 TaskManager 上其他作业的时段**执行。先记录
Job ID、成功 Checkpoint ID/path、上述 SQL 明细与 A/B 结果、Redis 版本，
并确认 Checkpoint 目录由两个 Flink 容器共享。不要删除容器卷。

```powershell
docker compose @compose kill taskmanager
docker compose @compose ps
docker compose @compose up -d taskmanager
docker compose @compose logs --tail=150 jobmanager
docker compose @compose logs --tail=150 taskmanager
$cp = Invoke-RestMethod "$rest/jobs/$job/checkpoints"
$cp.counts
$cp.latest.restored
$cp.latest.completed | Select-Object id,status,end_to_end_duration,external_path
(Invoke-RestMethod "$rest/jobs/$job").vertices | Select-Object name,status
```

同一 Job ID 下应看到 `restored.is_savepoint=false` 与恢复路径、
所有算子回到 `RUNNING`，并且产生**比故障前更新的成功 Checkpoint**。
等待 Kafka Lag/输入追平后，重新执行第 3 节的查询，比对 A/B 逐业务键
的 `click_count`/`revision`，Redis 窗口、修订号和完整排名；
没有新的成功快照时先查失败原因，不将 `RUNNING` 视为通过。
历史实际记录与结果见[演练记录](06-Day5-Checkpoint与Savepoint记录.md)。

## 5. 从指定的旧 Checkpoint 手动启动

这是**单独的受控重放演练**，不是第 4 节 TaskManager 故障所需步骤
（TaskManager 故障会自动恢复最近成功快照）。先在 Web UI/REST 取
`latest.completed.external_path`，或选择**确实保留**的更早 `chk-N`。
默认仅保留 3 个 Checkpoint，旧路径可能已被清理；不能凭历史日志推测
目录仍存在。选定快照与同一 JAR/消费组的状态要兼容。

```powershell
$sourceJob = '<原 Job ID>'
$chk = '<实际存在的 chk-N 目录名>'
$checkpoint = "file:///opt/flink/checkpoints/day5/$sourceJob/$chk/_metadata"
docker compose @compose exec jobmanager `
  ls -l "/opt/flink/checkpoints/day5/$sourceJob/$chk/_metadata"
```

先完成第 3 节的“前”快照；确认备份/回放可能重写外部结果后，
**取消唯一的原 Day 5 作业**，再启动新作业（新 Job ID）：

```powershell
docker compose @compose exec jobmanager flink cancel $sourceJob
docker compose @compose exec jobmanager `
  flink run -d -s $checkpoint -c Day5SinkJob `
  /opt/flink/usrlib/flink-rolesachieve-1.0-SNAPSHOT-all.jar
```

Checkpoint 的 `-s` 指向 `_metadata` 文件（不是凭目录名猜测）；
引用的状态文件也须保留并对两个 Flink 容器可见。
记录新 Job ID 和 `latest.restored.external_path`，待新 Checkpoint 成功、
输入追平后查询第 3 节并逐键比较。若 `chk-N/_metadata` 已不存在，
**停止本实验**，改用现场保留的可用快照或独立临时表 Sink 重试探针；
不拼接一个不存在的历史 ID。不可在原作业仍运行时另起回放写相同目标。

## 6. 创建 Savepoint，改非状态配置后恢复

保持原 Day 5 作业运行，执行并**复制命令返回的真实路径**：

```powershell
$sourceJob = $job
docker compose @compose exec jobmanager `
  flink savepoint $job file:///opt/flink/savepoints
$savepoint = '<返回的 file:/opt/flink/savepoints/savepoint-...>'
$savepointPath = $savepoint -replace '^file:', ''
docker compose @compose exec jobmanager ls -l "$savepointPath/_metadata"
docker compose @compose exec jobmanager flink cancel $job
docker compose @compose exec jobmanager `
  flink run -d -s $savepoint -c Day5SinkJob `
  /opt/flink/usrlib/flink-rolesachieve-1.0-SNAPSHOT-all.jar `
  --mysql-batch-size 100
```

`--mysql-batch-size 100` 是唯一的本次参数变化（默认 200），只影响
JDBC 提交阈值。Savepoint 路径不要自行按 Job ID 猜测；取消前先确认
创建命令成功且文件存在。恢复后把 `$job` 改为**新 Job ID**，重复第
2、3 节；检查 `latest.restored.is_savepoint=true`、路径正确、新
Checkpoint 成功。真实演练曾修复 JDBC 驱动加载错误并重新打包，
不是一次无错误的恢复；详情见记录文档。

不能随意改算子拓扑/UID、并行状态映射、状态名和类型/序列化器、
Kafka Topic/消费组/起始位点语义、规则窗口或去重 TTL。改动这些内容
应先做兼容性评估、用隔离环境恢复验证；不要用
`--allowNonRestoredState` 掩盖丢状态。

## 7. 重放/重试幂等验证

可用已保留的旧 Checkpoint 按第 5 节回放；没有旧快照时，使用
`Day5LiveSinkIntegrationTest` 的临时表/独立 Redis Key 探针：

```powershell
# Redis 独立 Key：旧窗口和旧 revision 被拒绝，新 revision 可覆盖。
$env:HOTNEWS_LIVE_REDIS_TEST = '1'
mvn -o -f flink-job/pom.xml -pl flink-rolesachieve -am `
  '-Dtest=Day5LiveSinkIntegrationTest' `
  '-Dsurefire.failIfNoSpecifiedTests=false' test
```

MySQL 探针需要管理员提供**宿主机可用的** `MYSQL_USER`、
`MYSQL_PASSWORD` 和 `MYSQL_DATABASE`，且先能交互登录；
在当前终端安全输入后设置 `HOTNEWS_LIVE_MYSQL_TEST=1` 再跑同一
测试。该探针对 MySQL `CREATE TEMPORARY TABLE ... LIKE` 写入
同键的 A 100/100/10 次、B revision 5/3/6 次，验证
行数仍为 1、A 最大点击 100、B 最大修订 6；不会写业务表。
不从容器导出口令、也不把口令写进命令行/笔记。

有宿主机连接凭据时，使用 `Get-Credential` 在本机输入而不把密码写进
命令历史，再分别采样（第 5/6 节新 Job ID 需替换）：

```powershell
$mysql = Get-Credential -Message 'MySQL 宿主机账户'
$env:MYSQL_USER = $mysql.UserName
$env:MYSQL_PASSWORD = $mysql.GetNetworkCredential().Password
$env:MYSQL_DATABASE = 'hotnews'
node tests/day5/capture.mjs before-replay-<唯一编号> $sourceJob
# 完成第 5/6 节的恢复后：
node tests/day5/capture.mjs after-replay-<唯一编号> <新JobID>
node tests/day5/compare.mjs `
  tests/day5/evidence/before-replay-<唯一编号>.json `
  tests/day5/evidence/after-replay-<唯一编号>.json
Remove-Item Env:MYSQL_PASSWORD
```

实际执行时把 `<唯一编号>` 与 `<新JobID>` 替换掉；
`capture.mjs` 需要宿主机和容器内访问权限，不保证
`docker compose exec mysql mysql -p` 成功就能从宿主机登录。
`<stage>` 应每次唯一，脚本不会覆盖现有证据。必须核对异常表增长、
C 的 `retracted` 行；目前 C 更新/撤销未携带单调修订号，
**任意旧 Checkpoint 对有活跃 C 告警的反向回放不能保证最终版本不倒退**，
因此该情形不得宣称全链路幂等通过。Redis Key 过期同样不能保证
跨过期拒绝旧窗口。固定批次最终 A=123/B=60 的核验，应只在
常驻作业停写后隔离执行 `Day5SinkJob --bounded`，不要同时写同一目标。

```powershell
# 必须先确认没有其他 RUNNING 的 Day5SinkJob，且本次 Topic 是预期固定输入。
docker compose @compose exec jobmanager `
  flink run -d -c Day5SinkJob `
  /opt/flink/usrlib/flink-rolesachieve-1.0-SNAPSHOT-all.jar `
  --bounded --mysql-batch-size 100
```

等待有界作业完成后，复用第 3 节查询验证明细 97044、最终 A 123、
B 60、活跃 C 0；若 Kafka Topic 中已混入其他批次，这些目标数无效。
`--bounded` 使用独立消费组并从 Topic 开始读，不能替代常驻作业。

不要执行 `docker compose down -v`：这会删除 Kafka、MySQL、Redis 卷；
也不要手工删除保存的 Checkpoint/Savepoint。现场已验证与待补项见
[记录](06-Day5-Checkpoint与Savepoint记录.md)，不能把预期步骤当实测。

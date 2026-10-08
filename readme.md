# 实时热点新闻检测系统

Flink 1.17 双流实时处理项目：持续消费 Kafka 文章/行为，清洗去重并按 `article_id` 关联；规则 A/C 产出 MySQL 告警，规则 B 产出 MySQL 历史榜单和 Redis 最新榜单。统一作业只做一次 Join，再复用 A/B/C 的实现。业务输入、窗口、表和测试现场见 [项目架构与数据字典](docs/01-项目架构与数据字典.md)。

## 目录与入口

| 位置 | 用途 |
| --- | --- |
| `flink-job/flink-common` | Kafka/JDBC/Redis、时间恢复、Checkpoint、指标工具 |
| `flink-job/flink-join` | `com.agd.flink.join.ArticleJoinBehavior`：双流清洗、去重、补关联和三类 Join 旁路 |
| `flink-job/flink-rules` | `com.agd.flink.roles.rolea/b/c/roleall.Achieve_roleA/B/C/All`；独立 A/B/C 会自行调用 Join |
| `generator/` | JSONL 数据与 Kafka 发送器、输入 Schema |
| `sql/` | MySQL 建表/迁移及查询；`tests/roles/`：独立 SQL 基准、边界、恢复与压测证据 |
| `deploy/`, `docs/` | Compose、启动脚本、考核笔记和故障 SOP |

| 入口 | 规则与输出 | IDEA 本地 Web UI |
| --- | --- | --- |
| `ArticleJoinBehavior` | 文章+行为 Join，`dirty_data`/`late_data`/`unmatched_behavior` 写 MySQL | 由本地运行配置决定 |
| `Achieve_roleA` | 5 分钟滑窗、每分钟滑动，`click > 1000` -> `article_alert` | `8083` |
| `Achieve_roleB` | 10 分钟滚动窗、`click+share+comment` 分类 Top5 -> `category_rank`、Redis | `8084` |
| `Achieve_roleC` | 同 IP 1 分钟内 >50 篇不同文章且平均阅读 <2000ms -> `ip_alert` | `10013` |
| `Achieve_roleAll` | 一次 Join + A/B/C，共用清洗流；8 盐分散热点 Join key | `10011` |

独立 A/B/C 与统一作业不要同时向同一批业务表写同一批输入。Docker JobManager Web UI 是 [localhost:8081](http://localhost:8081)，不使用 IDEA 的 `10011`。

## 启动

需要 Docker Desktop、JDK/Maven、Python；SQL 基准需要 Node.js，Kafka 发送需要 Python 的 `kafka-python`（详见 [生成器说明](generator/README.md)）。以下命令在**项目根目录 PowerShell** 执行：

```powershell
# 首次创建配置后，检查端口、密码和挂载路径；已有 deploy/.env 不要覆盖
Copy-Item deploy\.env.example deploy\.env
docker compose --env-file deploy/.env -f deploy/docker-compose.yml config
docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d
powershell -ExecutionPolicy Bypass -File deploy/create-kafka-topics.ps1
mvn -pl flink-job/flink-rules -am package -DskipTests
```

已有 `deploy/.env` 时跳过第一条。Compose 首次创建 MySQL 数据卷时会从 `sql/` 初始化表；**已有数据卷**必须按 [SQL 说明](sql/README.md)手动执行缺失的建表/迁移语句，改文件不会自动更新旧表。`deploy/start-all.ps1` 可一键启动基础服务，但不会编译、建 Kafka Topic 或提交业务作业。Compose 中 Checkpoint/Savepoint/RocksDB 目录绑定到 `D:/docker_data/flink/`；异机使用前先调整 `deploy/docker-compose.yml` 的挂载路径。宿主机 Kafka 用 `localhost:9092`，容器作业用 `kafka:29092`；MySQL 端口由 `deploy/.env` 的 `MYSQL_PORT` 决定，IDEA 默认 JDBC 为 `localhost:3307/hotnews`，Redis 默认 `localhost:6379`。

Docker 提交统一作业（确保本次要消费的数据和 MySQL 表属于同一批，避免与旧作业并行写入）：

```powershell
docker compose --env-file deploy/.env -f deploy/docker-compose.yml exec jobmanager `
  /opt/flink/bin/flink run -d `
  -c com.agd.flink.roles.roleall.Achieve_roleAll `
  /opt/flink/usrlib/rules/flink-rules-1.0-SNAPSHOT-all.jar
```

也可在 IDEA 使用 `flink-rules` 模块运行 `Achieve_roleAll`；本地需启用 provided 依赖以启动 MiniCluster Web UI。统一入口读取 `HOTNEWS_STATE_BACKEND`（Docker 默认 `rocksdb`，代码无变量时为 `hashmap`）、`HOTNEWS_CHECKPOINT_DIR`、`HOTNEWS_ROLE_ALL_GROUP`；JDBC 批量读取 `HOTNEWS_MYSQL_BATCH_SIZE`（默认 200）。只改环境文件不能热更新运行中的作业/容器；恢复与 Savepoint 步骤见 [故障 SOP](docs/06-故障演练与恢复SOP.md)。

## 数据与核对

生成器默认目标 500 篇文章、100000 条行为。只生成固定输入用 `--output json`；要同时落 JSONL 并写 Kafka 用 `--output both`，并保留同次生成的目录作为独立基准：

```powershell
py generator/generate_data.py --article-count 500 --behavior-count 100000 `
  --seed 20260927 --rate 2000 --output both `
  --output-dir generator/generator/my-run
```

已经存在固定批次 `generator/generator/total_data`；不要在其上重新生成并混用旧 Kafka Topic 的数据。Rule C 专项正例用 `--role-c-positive`。检查 SQL 建表和可单独查询的三类 Join 旁路见 [SQL 说明](sql/README.md)；Redis 最新榜单为 Hash `hotnews:top5:latest`（`ranking` 为 Top5 JSON，7200 秒未更新则过期）。

```powershell
node --no-warnings tests/roles/sql-check/program/verify.js `
  --role all --data-dir generator/generator/total_data `
  --flink-log tests/roles/sql-check/logs/role-all.log
```

上面的 `--data-dir` 与 `--flink-log` **必须是同次输入和作业输出**；示例路径不保证当前文件正好同批。窗口等待 Watermark 闭合后按业务键的最后版本比较；MySQL 全量查询可用 `tests/roles/sql-check/program/compare-mysql.mjs`，脚本说明见 [规则测试目录](tests/roles/README.md)。不要用 Flink 控制台打印次数代替业务条数。

## 验收记录

独立 A/B/C 的逐窗 SQL 核对、Rule A 阈值/窗口边界、当次 Redis/MySQL 最新榜单一致性有通过证据，见 [独立规则验收报告](tests/roles/summary/reports/rule-ab-acceptance-20261007.md)。Savepoint 非状态配置变更恢复、第二次 Kill TM 自动恢复和 Sink 幂等契约有通过记录，见 [Checkpoint 笔记](docs/04-Checkpoint%20与一致性笔记.md)。统一 RocksDB 固定输入 A/C 一致，但 B 缺最后两个窗口 9 行，见 [统一结果核对](tests/roles/state-backend/reports/role-all-result-check-20261008-rocksdb.md)；Day6 的 5000/s 目标档完成固定批次读取、CK 18 次成功，但未证明持续 >=2000/s，见 [压测记录](tests/roles/performance/reports/day6-5000-batch200-20261008.md)。这是不同批次/场景的结论，不能合并宣称统一作业全量结果与稳态吞吐均已通过。

完整笔记从 [01 架构与数据字典](docs/01-项目架构与数据字典.md) 和 [07 版本源码与代码审计](docs/07-版本源码与代码审计.md) 开始，02~06 位于 `docs/`。

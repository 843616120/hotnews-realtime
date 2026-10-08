# MySQL 建表与查询

项目默认数据库为 `hotnews`。本地 IDEA 的 JDBC 默认连接 `localhost:3307`、用户和密码为 `root`；可用 `MYSQL_HOST`、`MYSQL_PORT`、`MYSQL_DATABASE`、`MYSQL_USER`、`MYSQL_PASSWORD` 覆盖。Docker 容器内连接 `mysql:3306`。

| 文件 | 作用 |
| --- | --- |
| `01-hotnews-tables.sql` | `clean_behavior`、A/B/C 告警表、状态考核结果表及 `pipeline_event` |
| `02-dirty-data.sql` | Join JSON 解析失败、字段缺失等脏数据 |
| `03-late-data.sql` | 超出 Join 65 分钟允许迟到范围的数据 |
| `04-unmatched-behavior.sql` | 先到行为与补关联记录，`matched` 标明是否已关联 |
| `05-rule-c-ip-alert.sql` | 已存在的旧版 `ip_alert` 表增列迁移 |

在已有 MySQL 数据卷中手动执行缺失的建表/迁移语句；Compose 初始化挂载只会在首次创建数据卷时自动执行。不要为了执行 SQL 清空数据库或 Kafka。SQL 文件不会自行启动 Flink 作业。

规则结果按业务唯一键批量幂等 UPSERT：A 为 `(window_start_ms, article_id)`，B 为 `(window_start_ms, rank_no)`，C 为 `(alert_minute_ms, ip)`。B 的最新榜单另存 Redis `hotnews:top5:latest`，过期时间两小时；MySQL 保存每个窗口的结果供逐窗核对。规则自己的超期数据按 `ROLE_A_LATE`、`ROLE_B_LATE`、`ROLE_C_LATE` 写入 `pipeline_event`。

```sql
SELECT * FROM article_alert ORDER BY window_start_ms, article_id LIMIT 20;
SELECT * FROM category_rank ORDER BY window_start_ms, rank_no LIMIT 20;
SELECT * FROM ip_alert WHERE retracted = FALSE ORDER BY alert_minute_ms, ip LIMIT 20;
SELECT * FROM pipeline_event WHERE event_type IN ('ROLE_A_LATE','ROLE_B_LATE','ROLE_C_LATE');
```

Join 的三条独立旁路可使用 `queries/join-check.sql` 查询。离线 SQL 基准见
`tests/roles/sql-check/README.md`，不要将不同 Kafka 批次和 JSONL 目录混合比较。

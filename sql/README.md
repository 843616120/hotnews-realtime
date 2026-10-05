# SQL 文件

这里是普通 SQL 文件目录，不是 Maven 模块。

`01-day5-tables.sql` 创建清洗明细、规则 A/B/C 结果和异常记录五张表，
JDBC 参数化批量 UPSERT 位于 `Day5MySqlSink`；`queries/day5-check.sql`
用于核对清洗明细、告警和未解决的待回放事件，不会作为初始化脚本自动执行。
`deploy/docker-compose.yml` 将本目录挂载到 MySQL 初始化目录，仅首次创建
数据卷时自动执行根目录的 `.sql`。已有数据卷需手动执行新建表 SQL，切勿
为了加载脚本而清除已有 Kafka/MySQL/Redis 数据卷。

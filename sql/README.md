# SQL 文件

这里是普通 SQL 文件目录，不是 Maven 模块。

建议后续放入：

```text
01-create-database.sql
02-create-tables.sql
03-upsert.sql
04-baseline.sql
05-check-result.sql
```

内容应覆盖 MySQL 表结构、批量幂等 UPSERT、基准结果和结果核对 SQL。

`deploy/docker-compose.yml` 会把这个目录挂载到 MySQL 的初始化目录。MySQL 数据目录第一次创建时，会自动执行其中的 `.sql` 文件。

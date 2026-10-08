# 统一规则作业

统一入口为 `com.agd.flink.roles.roleall.Achieve_roleAll`，本地 Web UI 端口为
`8086`。

该作业只创建一次 Article/Behavior Join 和一次清洗明细 Sink，然后把清洗后的
行为同时送入规则 A、B、C。三条规则沿用各自已经通过校验的窗口、事件时间
Watermark 和 65 分钟迟到等待配置：

- A：五分钟滑动窗口，结果写入 MySQL `article_alert`。
- B：十分钟滚动窗口，结果写入 MySQL `category_rank`，最新榜单写入 Redis。
- C：同 IP 一分钟回看，结果写入 MySQL `ip_alert`。

三条规则的迟到旁路仍按 `ROLE_A_LATE`、`ROLE_B_LATE`、`ROLE_C_LATE` 写入
`pipeline_event`。Join 层的脏数据、迟到数据和未匹配数据仍由 Join 作业原有的
旁路处理。

统一作业输出可使用同一份输入执行全部 SQL 基准：

```powershell
node --no-warnings tests\roles\sql-check\program\verify.js `
  --role all `
  --data-dir generator\generator\role3_data `
  --flink-log C:\path\to\role-all.log
```

`--role all` 会分别执行 `tests/roles/sql-check/sql/role_a.sql`、
`tests/roles/sql-check/sql/role_b.sql` 和 `tests/roles/sql-check/sql/role_c.sql`，
因此 A、B、C 的字段和窗口仍然单独核对。

# 统一规则作业

统一入口为 `com.agd.flink.roles.roleall.Achieve_roleAll`，本地 Web UI 端口为
`10012`，Docker JobManager 仍为 `8081`。本地通过
`createLocalEnvironmentWithWebUI` 创建环境时传入 REST 配置，不用
`env.configure` 修改 REST 端口。

该作业只创建一次 Article/Behavior Join 和一次清洗明细 Sink，然后把清洗后的
行为同时送入规则 A、B、C。三条规则沿用各自已经通过校验的窗口、事件时间
Watermark 和 65 分钟迟到等待配置：

- A：五分钟滑动窗口，结果写入 MySQL `article_alert`。
- B：十分钟滚动窗口，结果写入 MySQL `category_rank`，最新榜单写入 Redis。
- C：同 IP 一分钟回看，结果写入 MySQL `ip_alert`。

## 两阶段 B 与超期补算

B 第一阶段按 category+event_id 盐分片，十分钟窗口并行度 3；第二阶段按窗口起点
合并最新分片快照，并行度 2。同一窗口的完整排名仍由一个子任务生成，不是将
全局 Top5 拆成两个不完整榜单。分片快照替换而非重复累加；分类和文章的并列
排序、top_articles 和 revision 含义不变。MySQL 按窗口+名次分区并行度 2，
与聚合拆链；Redis 仍用 Lua 原子写完整榜单。当前不加中间 Kafka。

A/B 使用事件时间确定窗口，一秒处理时间定时器只批量触发累计结果，不改变
窗口归属或生成处理时间 Watermark。最后一批结果不必等待下一批数据。

统一作业的 RuleReplaySink 检查每个成功 Join 的清洗行为影响的窗口，并接收
三条规则真实的 late 旁路。已经超期
时先存补算请求，成功 Checkpoint 后（所有清洗 Sink 已 flush）从 clean_behavior
查询完整窗口数据，复用 A/B/C 的计算方法 UPSERT；A 部分过期滑窗也会补算。
补算输出使用原 ROLE_A/B/C 标签，原 SQL/日志校验脚本可以识别。失败恢复时
请求随 Checkpoint 恢复，重复重算是覆盖而不是累加。

启动前先手动执行 sql/06-clean-behavior-replay.sql，增加 event_time_ms、
article_version 和时间索引。旧表数据须通过同批次重新消费填充这两列，
不能用迁移的默认值当成完整历史。此功能改变了状态和拓扑，首次运行不要
恢复旧 Savepoint；停止旧作业，用新消费组从同一批 Kafka 重跑。
IDEA 环境变量示例：HOTNEWS_ROLE_ALL_GROUP=hotnews-all-two-stage-run1；
默认组仍保留原值，必须主动换组才会重新从 earliest 读取行为。

全量校验要等 Source 读完、A/B 两级输出静止以及之后至少一次成功 Checkpoint
并完成补算。不混用其他批次表数据；超期补算时间算在端到端延迟内。
状态 TTL 仍有限，并非任意年限的乱序都保存在内存中；超出在线保留范围使用
已持久化清洗明细重算。编译通过不等于本次吞吐和数据库一致性已实测通过。

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

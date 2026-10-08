# Rule C

在 MySQL 执行 `sql/05-rule-c-ip-alert.sql` 的完整文件，然后在 IDEA 启动
`com.agd.flink.roles.rolec.Achieve_roleC`。本地 Web UI 为 <http://localhost:10013>。
清洗明细和 Join 旁路表沿用已用于 A/B 的建表文件和连接参数。

只处理 Join 后的 click，沿用 A/B 的行为事件时间与 Watermark。每次点击回看
`(event_time - 60秒, event_time]`，不同文章数严格大于 50，平均阅读时长
严格小于 2000ms 才告警；重复点击参与平均值，但不增加不同文章数。

IP 状态有两部分：待补算点击，以及每分钟的回看状态。回看状态保存文章 ID
集合、点击数、总阅读时长、起止时间和告警修订号。同 IP 同分钟保留最早
满足条件的告警，迟到点击重新核算：修正时 UPSERT；不再满足时输出撤销。
同一毫秒的点击一起计算。没有命中的 IP 也会保存回看状态。

两部分状态均设置处理时间 TTL 一小时，符合考核的 IP 状态要求。
事件时间额外保留 65 分钟补算，每分钟的事件时间定时器清理过期回看；一条
点击不再影响所在分钟和下一分钟时删除。TTL 增量清理每次最多检查 100 个条目，
这是清理批次大小，不改变一小时 TTL。TTL 是闲置清理，不是周期性的全量删除。
处理时间上条目超过一小时未更新会不可见，所以连续暂停一小时以上后不能保证
此前点击仍可补算；65 分钟事件时间保留不等于 65 分钟墙钟时间保留。

MySQL 告警表为 `ip_alert`，主键 `(alert_minute_ms, ip)`。有效告警带文章集合、
计数、总时长、平均值、时间范围；撤销行保留主键和修订号，统计字段置空。
`ROLE_C_LATE` 写入 `pipeline_event`。一条点击仅部分回看已过期时，旁路会留痕，
剩余未过期回看仍参与补算；全部过期时只留旁路。

SQL 对照仍使用 `tests/roles/sql-check/sql/role_c.sql`，其 2000ms 阈值和逐点击回看口径不变。
只核对同批输入及已经完成补算的时间范围。生成器使用大量分散 IP，当前样本
没有告警也可能是正确结果；不能仅凭零行证明正例链路已验证。

```sql
SELECT * FROM ip_alert WHERE retracted = FALSE ORDER BY alert_minute_ms, ip;
SELECT * FROM pipeline_event WHERE event_type = 'ROLE_C_LATE';
```

# MySQL 与 Redis 核验

本目录集中保存存储一致性、批量幂等 UPSERT、Redis Key/Value/TTL 的程序和证据。

| 分类 | 内容 |
| --- | --- |
| `program/RankingRead.java` | 只读 `category_rank` 和 `hotnews:top5:latest`，不读取 Kafka |
| `program/LiveEvidence.java` | 含临时 MySQL 表、专用 Redis Key 的契约测试；也含非本次范围的取证入口 |
| `program/run-sinks.ps1`、`verify-sinks.mjs` | 显式运行 Sink 契约和临时表独立 SQL 对照 |
| `program/analyze-live.mjs` | 离线分析调用者提供的完整存储/输入快照，输出 SQL 与结果差异 |
| `evidence/current-ranking/` | 2026-10-07 15:11 的只读快照和逐字段比较 JSON |

已采集证据显示该时刻最新窗口 `[01:40,01:50)` 的五个分类一致，
revision=1199、TTL=5992 秒、字段差异=0；这是采集时刻结果，不是实时查询值。
已删除的旧幂等测试证据不由该快照替代。

Sink 契约入口：

```powershell
.\tests\roles\sink-check\program\run-sinks.ps1 `
  -RuleRunId 已有边界规则证据目录名 `
  -RunId 本次唯一名称
```

该命令从 `boundary-test/evidence` 读取规则输出；连接级临时 MySQL 表自动消失，
Redis 只写 `hotnews:acceptance:ab:<RunId>`，TTL=7200，不删除业务 Key。
这是会写临时数据的测试，不是只读核验。
较小 revision、旧窗口拒绝、同窗口修订和自然到期须看相应专项证据，
不能由单次静态快照声称全部验证。

2026-10-08 已执行证据：`evidence/20261008-day5-sink-idempotence/`。
批量重放、A 的较小计数拒绝、B 的乱序重放、clean_behavior 的 event_id 去重、
Redis revision/旧窗口/TTL 和生产表未修改均通过。该目录中的边界样本独立 SQL
对照仍有 A 的 5 个字段差异，这是样本结果与独立基准的差异，不影响上述 Sink
幂等契约断言，不能写成生产全量 SQL 已通过。

`analyze-live.mjs` 需要完整 `live-snapshot.json`、表快照、已导出的输入和
源码基准；本目录的单次榜单快照不足以执行它。该程序不主动读取 Kafka，
不作为本次要求的上游验证入口；迟到分析使用 `sql-check/program/audit-late.mjs`。
依赖已删除历史快照的旧差异入口已移除。

迁移映射：原 `ab-acceptance/java/RankingRead.java`、`LiveEvidence.java` 和
Sink 脚本在本目录 `program`；原 `20261007-ranking-live` 快照在
`evidence/current-ranking`，文件内容和采集时间未改变。

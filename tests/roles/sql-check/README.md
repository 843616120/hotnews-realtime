# 独立 SQL 与结果核对

本目录完成考核中的“从原始 JSONL 独立清洗、去重、关联和逐窗口核对”。

| 分类 | 文件与用途 |
| --- | --- |
| `program/verify.js` | 核对 A 的窗口+文章、B 的窗口+名次、C 的 IP+告警分钟 |
| `program/baseline.test.js` | 校验 SQL 基准和日志差异报告程序 |
| `sql/prepare.sql` | 独立清洗、去重和文章关联 |
| `sql/role_a.sql`、`role_b.sql`、`role_c.sql` | 三条规则的独立预期值 |
| `logs/` | 单规则和统一作业的原始日志 |
| `evidence/total-data/verification.txt` | 2026-10-07 的 A=17、B=5 对照记录 |

```powershell
node --no-warnings tests\roles\sql-check\program\verify.js `
  --role a --through 2026-09-27T00:10:00Z `
  --data-dir generator\generator\total_data `
  --flink-log tests\roles\sql-check\logs\role-a.log

node --no-warnings --test tests\roles\sql-check\program\baseline.test.js
```

日志采用逐行读取，迟到修订按业务键合并。默认 JSONL 目录是
`generator/generator/data`；验收必须显式选定本批 `--data-dir`。
用 `--sqlite` 可保存到尚不存在的 SQLite 文件，不覆盖旧数据库。
`--through` 不代替窗口 Watermark 证据。

对已经采集的 MySQL 结果做固定输入全量逐字段比较：

```powershell
node --no-warnings tests\roles\sql-check\program\compare-mysql.mjs `
  --data-dir generator\generator\total_data `
  --snapshot tests\roles\recovery\evidence\20261008-131831-505-final-before-validation `
  --output tests\roles\sql-check\evidence\20261008-day5-final\mysql-vs-sql.json
```

2026-10-08 的报告在 `evidence/20261008-day5-final/mysql-vs-sql.json`：
A 为 SQL=123、MySQL=25，B 为 SQL=64、MySQL=55，C 为 SQL=1、MySQL=1；
报告还包含缺失、多出和逐字段差异，不能只按行数判断通过。

`program/audit-late.mjs` 读取调用者提供的 `live-analysis.json` 和 SQLite，
不主动访问 Kafka；快照分析入口位于 `../sink-check/program/analyze-live.mjs`。
缺少完整原始快照时不能执行这些补充分析。
本次目录整理没有重新采集输入、数据库或日志。

2026-10-08 搬迁后的寻址复查中，当前 SQL 清洗得到 95082 条行为，
与 2026-10-07 记录的 97044 条不同；截至 `00:10Z`，A 的 17 行、B 的 5 行
虽然业务键齐全，但结果字段存在差异。原因未在目录整理中调查，
不能把保留的旧通过记录当作当前代码和输入的重新验收结论。

迁移映射：原 `tests/roles/verify.js`、`prepare.sql`、`role_*.sql` 分别位于
本目录 `program/verify.js`、`sql/prepare.sql`、`sql/role_*.sql`；
原 `tests/roles/role-*.log` 位于本目录 `logs/`。
证据内记录的原执行命令不改写，复查时使用上述新命令。

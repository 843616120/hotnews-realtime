# 规则考核资料

按考核要求归类，每个目录集中保存对应的程序、SQL、输入、日志、证据和说明，
不再按 Rule A/B/C 拆散同一项考核资料。

| 目录 | 考核内容 | 主要资料 |
| --- | --- | --- |
| `sql-check/` | 从原始 JSONL 独立清洗、去重、关联，逐窗口核对规则结果 | `program/`、`sql/`、`logs/`、`evidence/` |
| `boundary-test/` | 阈值、窗口边界、去重、重叠窗口、Top 排名及迟到专项 | `program/`、`inputs/`、`sql/`、`evidence/` |
| `sink-check/` | MySQL/Redis 一致性、批量幂等、Key/Value/TTL | `program/`、`evidence/` |
| `state-backend/` | HashMap/RocksDB 运行结果与状态后端资料 | `logs/`、`reports/` |
| `summary/` | 总验收报告和证据索引 | `reports/` |

每类目录的 `README.md` 提供程序入口、证据位置和验证范围。
生产固定输入仍在 `generator/generator`，不复制或覆盖现有 JSONL。

在项目根目录运行全规则结果核对：

```powershell
node --no-warnings tests\roles\sql-check\program\verify.js `
  --role all `
  --data-dir generator\generator\total_data `
  --flink-log tests\roles\sql-check\logs\role-all.log
```

单规则使用 `--role a|b|c`；只算独立基准时省略 `--flink-log`。
`--through ISO时间` 仅限制比较范围，不证明 Watermark 已到达或补算已经完成。
迟到修订按业务键合并，不以日志打印行数作为输入数量。

2026-10-08 目录整理只移动资料、更新程序寻址和使用说明，没有重新执行生产作业。
原始证据中的采集时间、结果和当时的绝对路径原样保留；目录映射见各类说明。
当前全量复查必须以实际输入文件哈希和同批日志为准；若哈希或日志批次不同，
不得把历史报告中的通过数字直接套用到当前运行。

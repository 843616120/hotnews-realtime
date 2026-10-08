# 阈值与窗口边界专项

本目录集中保存 A/B 小样本、独立 SQL、真实规则算子运行程序及可复查证据。

| 分类 | 内容 |
| --- | --- |
| `program/RulesAbAcceptance.java`、`run.ps1` | 内存事件流执行真实 A/B 去重与规则算子，先 A 后 B |
| `program/fixtures.mjs` | 在不存在的独立目录生成样本，不覆盖业务 JSONL |
| `program/verify.mjs` | A/B 窗口、排名、迟到修正的 SQL 对照 |
| `program/verify-a-threshold-boundary.mjs` | A 的 999/1000/1001 和 end 前/本身/后 1ms 专项 |
| `inputs/20261007-ab/` | A/B 文章、行为 JSONL 和显式 Watermark 日程 |
| `sql/` | 独立清洗、A/B 窗口 SQL 和 A 阈值边界 SQL |
| `evidence/20261007-rule-a-threshold-boundary/` | 实际输出、运行日志、源码 SHA-256 和逐项检查 |

证据中的 `threshold-boundary-verification.json` 为 A 阈值和结束边界专项：
24 项通过，SQL=18、Flink=18、字段差异=0。
同目录的 `verification.json` 是更广的 A/B 检查，不可用前者替代其失败项：
当次 A 源码允许迟到 65 分钟，而该广域 SQL/清理断言为 50 分钟，留下了真实差异。
此处只整理路径，没有更改迟到配置、断言或已有测试结论。

重新运行真实算子（必须使用新的 RunId）：

```powershell
.\tests\roles\boundary-test\program\run.ps1 -RunId 本次唯一名称
node --no-warnings tests\roles\boundary-test\program\verify-a-threshold-boundary.mjs `
  tests\roles\boundary-test\evidence\本次唯一名称
```

专项程序拒绝覆盖已存在的检查 JSON。程序不连接 Kafka/MySQL/Redis，不执行生产 main，
不声称覆盖双流 Join、完整 ETL 或生产 Sink。
编译仅写证据目录，使用现有规则依赖 JAR，不写共享 `target`。

迁移映射：原 `tests/roles/ab-acceptance/inputs`、`sql` 及边界证据分别移到
本目录 `inputs`、`sql`、`evidence`；原 Java 与运行/校验脚本在 `program`。
已有证据的原始绝对路径、输入哈希和原始结果保留不变。

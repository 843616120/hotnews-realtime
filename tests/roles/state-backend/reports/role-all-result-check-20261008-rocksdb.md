# 统一作业结果核对表

## 核对范围

| 项目 | 内容 |
| --- | --- |
| 固定输入目录 | `D:\study\study_flink\hotnews-realtime\generator\generator\total_data` |
| Flink 日志 | `tests/roles/state-backend/logs/state-rocksdb-20261007.log` |
| 状态后端 | RocksDB，`frocksdbjni 6.20.3-ververica-1.0` |
| 核对脚本 | `tests/roles/sql-check/program/verify.js`，已改为逐行流式读取 JSONL 和 Flink 日志 |
| 数据库 | MySQL `localhost:3307/hotnews` |
| 数据库快照 | 只读 JDBC 查询，2026-10-08 本次运行后采集 |

## 固定输入

| 数据阶段 | 条数 |
| --- | ---: |
| `article_raw` | 500 |
| `article_clean` | 500 |
| `behavior_raw` | 100000 |
| `behavior_clean` | 95082 |
| `joined` | 95082 |

固定输入通过 `tests/roles/sql-check/sql/prepare.sql` 完成清洗、去重和 Join，未使用 Flink 输出生成基准。

## 规则结果核对

| 规则 | SQL 基准结果 | Flink 控制台输出 | 数据库查询结果 | 缺失 | 多出 | 字段差异 | 结论 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Rule A `article_alert` | 123 | 123 | 123 | 0 | 0 | 0 | 一致 |
| Rule B `category_rank` | 64 | 55 | 55 | 9 | 0 | 0 | 尚未一致 |
| Rule C `ip_alert` | 1 | 1 | 1 | 0 | 0 | 0 | 一致 |

### Rule B 缺失窗口

Flink 和 MySQL 都缺少以下两个 SQL 基准窗口：

| 窗口 | SQL 基准行数 | Flink 行数 | 缺失 |
| --- | ---: | ---: | ---: |
| `2026-09-27T01:50:00Z ~ 02:00:00Z` | 5 | 0 | 5 |
| `2026-09-27T02:00:00Z ~ 02:10:00Z` | 4 | 0 | 4 |

其余 11 个 Rule B 窗口均为每个窗口 5 行，字段核对通过。当前报告按作业日志和数据库快照记录，不把缺失窗口伪装成一致。

## 数据库查询结果

| MySQL 表 | 查询行数 | 说明 |
| --- | ---: | --- |
| `clean_behavior` | 95082 | 与 `behavior_clean`、`joined` 数量一致 |
| `article_alert` | 123 | 与 Rule A Flink 输出一致 |
| `category_rank` | 55 | 与 Rule B Flink 输出一致，少于 SQL 基准 9 行 |
| `ip_alert` | 1 | 与 Rule C Flink 输出一致 |
| `dirty_data` | 998 | 本次脏数据旁路记录 |
| `late_data` | 372 | Join 迟到旁路记录，当前数量较少 |
| `unmatched_behavior` | 14 | Join 补算/未匹配审计记录 |
| `pipeline_event` | 0 | 本次没有规则 A/B/C 的超期审计写入 |

数据库查询使用只读 JDBC 连接，没有执行清空、更新或删除。当前生产表没有 `job_id` 或 `run_id` 字段，因此异常表的数量只能作为本次运行后的数据库快照；若运行前没有清空异常表，不能仅凭表总数证明全部记录都来自本次作业。

## 核对命令

固定输入和 Flink 日志核对：

```powershell
node --no-warnings tests/roles/sql-check/program/verify.js `
  --role all `
  --flink-log "D:\study\study_flink\hotnews-realtime\tests\roles\state-backend\logs\state-rocksdb-20261007.log" `
  --data-dir "D:\study\study_flink\hotnews-realtime\generator\generator\total_data"
```

本次旧版脚本因为一次性读取约 800 MB 日志触发 Node.js `ERR_STRING_TOO_LONG`。修改后使用 `readline.createInterface` 逐行读取，固定输入和结果数量均已成功核对。

## 结论

本次 RocksDB 运行中迟到数据明显减少，Rule A 和 Rule C 与固定 SQL 基准一致，Rule B 的前 11 个窗口一致；但 Rule B 最后两个窗口共缺少 9 行，统一作业整体结果尚未通过全量一致性验收。

当前主要问题不是 Node 校验脚本，而是 Rule B 在最后两个事件时间窗口没有产生结果。需要继续确认作业结束前的 Watermark 是否推进到 `02:10` 窗口结束、窗口数据是否被判定为 late，以及 Checkpoint 超时/作业结束是否阻止了最后窗口输出。

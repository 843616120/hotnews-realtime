# 状态后端资料

集中保存 HashMap/RocksDB 的运行日志和统一作业结果报告，不复制生产状态作业。

| 分类 | 内容 |
| --- | --- |
| `logs/state-hashmap-20261007.log` | HashMap 运行原始日志 |
| `logs/state-rocksdb-20261007.log` | RocksDB 运行原始日志 |
| `reports/role-all-result-check-20261008-rocksdb.md` | 独立 SQL、日志及数据库对照报告 |

使用 `sql-check` 的共用程序核对，不另建一份校验器：

```powershell
node --no-warnings tests\roles\sql-check\program\verify.js `
  --role all `
  --data-dir generator\generator\total_data `
  --flink-log tests\roles\state-backend\logs\state-rocksdb-20261007.log
```

报告中的 A/C 一致和 B 尾窗候选未产出的结论原样保留。
是否关窗须核验实际 Watermark，不能因目录整理改成全量通过。
独立状态程序仍在 `flink-job/flink-state/src/test`，本次不修改或执行。

迁移映射：原 `tests/roles/state-*.log` 移到本目录 `logs`，
原 `tests/roles/role-all-result-check-20261008-rocksdb.md` 移到 `reports`。

# 测试资料

`tests/` 按应用职责分类，不使用 day 命名目录，不需要 `pom.xml`。

当前保留：

```text
tests/
  roles/
    sql-check/
      program/
      sql/
      logs/
      evidence/
    boundary-test/
      program/
      inputs/
      sql/
      evidence/
    sink-check/
      program/
      evidence/
    state-backend/
      logs/
      reports/
    summary/
      reports/
  README.md
```

当前 A/B 全量核对输入为 `generator/generator/total_data`。
原有独立样本 `data`、`role2_data` 和边界测试样本按各自证据说明使用，
不能与全量批次混用。执行方式见各考核目录的 `README.md`，
总结果见 `roles/summary/reports/rule-ab-acceptance-20261007.md`。

Rule C 正例位于 `generator/generator/role3_data`。规则 SQL、统一作业校验及 A/B
验收资料按考核要求归类在 `roles/`；独立状态作业测试位于
`flink-job/flink-state/src/test/`。

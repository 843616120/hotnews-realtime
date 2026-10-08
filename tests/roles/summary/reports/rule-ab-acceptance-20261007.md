# Rule A / Rule B / Rule C 当前验收报告

验收日期：2026-10-07，Asia/Shanghai。  
项目：`D:\study\study_flink\hotnews-realtime`

## 结论

本次当前记录中：

- Rule A 独立 SQL 与 Flink 日志逐窗口、逐文章核对通过。
- Rule B 独立 SQL 与 Flink 日志逐窗口、逐名次核对通过。
- Rule C 独立 SQL、Flink 日志与 MySQL `ip_alert` 端到端核对一致。
- 当前 MySQL `category_rank` 与 Redis `hotnews:top5:latest` 最新榜单逐字段一致。
- 本次未清空 MySQL/Redis，未删除业务 Key，未重置消费位点，未向业务 Topic 写入数据。

## 输入与运行范围

固定输入目录：

`D:\study\study_flink\hotnews-realtime\generator\generator\total_data`

| 项目 | 实际值 |
| --- | ---: |
| `article_raw` | 500 |
| `article_clean` | 500 |
| `behavior_raw` | 100000 |
| `behavior_clean` | 97044 |
| `joined` | 97044 |

输入文件 SHA-256：

```text
article_stream.jsonl
AC5CE6944C1E2D4C045A7E0BA13BC2829C308EEC5A49612C8ABF918C9449C395

behavior_stream.jsonl
47F6048ED92D27AD40494355CBC5EFF90B257FBAC728BCF7915B407738ADA213
```

上游链路按用户确认通过，本次不再把 Kafka 中是否属于 A/B 输入作为阻塞项。

## Rule A

当前源码配置：click；事件时间 5 分钟滑动窗口；步长 1 分钟；点击数严格大于
1000 才输出；当前源码允许迟到 65 分钟。

执行命令：

```powershell
node --no-warnings tests\roles\sql-check\program\verify.js --role a `
  --flink-log "D:\study\study_flink\hotnews-realtime\tests\roles\sql-check\logs\role-a.log" `
  --through 2026-09-27T00:10:00Z `
  --data-dir "D:\study\study_flink\hotnews-realtime\generator\generator\total_data"
```

| 项目 | 结果 |
| --- | ---: |
| SQL 基准 | 17 行 |
| Flink 实际 | 17 行 |
| 缺失 | 0 |
| 多出 | 0 |
| 字段差异 | 0 |

覆盖 9 个窗口：
`2026-09-26T23:57:00Z` 至 `2026-09-27T00:10:00Z`。
每个窗口的窗口边界、article_id、title、category 和 click_count 均一致。

### Rule A 阈值与结束边界专项

使用独立小样本和独立 SQL，从原始 JSONL 清洗、按 `event_id` 去重、关联文章后，
再与当前 `Achieve_roleA.buildRule` 的实际输出比较。该专项没有写入 Kafka、MySQL
或 Redis。

| 检查项 | 预期 | 实际 | 状态 |
| --- | --- | --- | --- |
| 999 次 click | 五个窗口均为 999，不告警 | 5 个窗口均为 999，告警 0 个 | 通过 |
| 1000 次 click | 不告警 | 告警 0 个 | 通过 |
| 1001 次 click | 5 个重叠窗口各告警 1001 | 5 个窗口均告警 1001 | 通过 |
| share/comment 不增加 click | `count-999`、`count-1000` 仍不超过阈值 | 两类行为存在，click 计数未增加 | 通过 |
| 重复 `event_id` | `count-1000` 去重后仍为 1000 | 去重后 1000，未产生告警 | 通过 |
| `end-1ms`：00:04:59.999 | 进入 `[00:00,00:05)` | 1000+1，5 个窗口告警 | 通过 |
| `end`：00:05:00.000 | 不进入 `[00:00,00:05)` | 该窗口不告警，只有 4 个窗口告警 | 通过 |
| `end+1ms`：00:05:00.001 | 不进入 `[00:00,00:05)` | 该窗口不告警，只有 4 个窗口告警 | 通过 |
| 五重叠窗口起点 | `00:00`、`00:01`、`00:02`、`00:03`、`00:04` | 实际完全一致 | 通过 |
| 独立 SQL 与实际输出 | 18 个正例业务键，字段差异 0 | SQL=18、Flink=18、差异=0 | 通过 |

专项运行证据：
`tests/roles/boundary-test/evidence/20261007-rule-a-threshold-boundary/threshold-boundary-verification.json`。
该证据还记录了输入文件行数、SHA-256、当前生产源码运行前后未变化，以及六个样本均在
首次 Watermark 前到达。

## Rule B

当前源码配置：事件时间 10 分钟滚动窗口；click、share、comment 各计 1；
分类 Top 5；每类 `top_articles[]` 最多 5 篇。

执行命令：

```powershell
node --no-warnings tests\roles\sql-check\program\verify.js --role b `
  --flink-log "D:\study\study_flink\hotnews-realtime\tests\roles\sql-check\logs\role-b.log" `
  --through 2026-09-27T00:10:00Z `
  --data-dir "D:\study\study_flink\hotnews-realtime\generator\generator\total_data"
```

| 项目 | 结果 |
| --- | ---: |
| SQL 基准 | 5 行 |
| Flink 实际 | 5 行 |
| 缺失 | 0 |
| 多出 | 0 |
| 字段差异 | 0 |

覆盖窗口：
`[2026-09-27T00:00:00Z, 2026-09-27T00:10:00Z)`。
分类名次、category、score、revision、文章排序和 `top_articles[]` 均一致。

## Rule C

固定输入目录：

`D:\study\study_flink\hotnews-realtime\generator\generator\role3_data`

| 项目 | 实际值 |
| --- | ---: |
| `article_raw` | 500 |
| `article_clean` | 500 |
| `behavior_raw` | 100000 |
| `behavior_clean` | 95082 |
| `joined` | 95082 |

执行命令：

```powershell
node --no-warnings tests\roles\sql-check\program\verify.js --role c `
  --flink-log "D:\study\study_flink\hotnews-realtime\tests\roles\sql-check\logs\role-c.log" `
  --data-dir "D:\study\study_flink\hotnews-realtime\generator\generator\role3_data"
```

| 项目 | 结果 |
| --- | ---: |
| SQL 基准 | 1 行 |
| Flink 实际 | 1 行 |
| 缺失 | 0 |
| 多出 | 0 |
| 字段差异 | 0 |

ROLE_C：SQL 基准 1 行  
`2026-09-27T02:00:00.000Z`：SQL=1 Flink=1 缺失=0 多出=0 字段差异=0

唯一告警分钟为 `2026-09-27T02:00:00.000Z`，IP 为
`10.255.255.1`，文章数为 51，点击数为 51，平均阅读时长为 1000 ms。

## MySQL 端到端一致性

使用固定输入独立 SQL、Rule C Flink 日志和 MySQL `ip_alert` 快照进行逐字段核对。
MySQL 端保持只读，没有清空、删除或修改业务数据。

| 规则 | SQL 基准 | Flink 实际 | MySQL 实际 | 缺失 | 多出 | 字段差异 | 状态 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Rule C `ip_alert` | 1 | 1 | 1 | 0 | 0 | 0 | 通过 |

MySQL 端核对的业务键为告警分钟和 IP；`article_count`、`click_count`、
`avg_read_duration_ms`、`article_ids`、`window_start_ms`、`window_end_ms`
均与独立 SQL 和 Flink 结果一致。因此本批 Rule C 的
“固定输入 → 独立基准 → Flink 结果 → MySQL 结果”端到端验收通过。

## 当前 MySQL / Redis 一致性

使用只读程序读取当前结果：

- MySQL：`localhost:3307/hotnews`，表 `category_rank`
- Redis：`localhost:6379`
- Redis Key：`hotnews:top5:latest`

| 检查项 | 实际结果 | 状态 |
| --- | --- | --- |
| MySQL `category_rank` 总行数 | 55 | 通过 |
| MySQL 最新窗口 | `[2026-09-27T01:40:00Z, 2026-09-27T01:50:00Z)`，5 行 | 通过 |
| Redis Key | `hotnews:top5:latest` | 通过 |
| Redis 类型 | `hash` | 通过 |
| Redis Hash 字段 | `window_start_ms`、`window_end`、`revision`、`ranking` | 通过 |
| Redis 窗口 | `[2026-09-27T01:40:00Z, 2026-09-27T01:50:00Z)` | 通过 |
| Redis revision | 1199，与 MySQL 最新窗口一致 | 通过 |
| Redis ranking | 5 个 rank，与 MySQL 的 category、score、top_articles 顺序及字段一致 | 通过 |
| Redis TTL | 5992 秒，满足 `0 < TTL <= 7200` | 通过 |
| Redis/MySQL 字段差异 | 0 | 通过 |

当前一致性结论：**通过**。

## 证据

- [total_data 规则核对记录](D:\study\study_flink\hotnews-realtime\tests\roles\sql-check\evidence\total-data\verification.txt)
- [当前 MySQL/Redis 只读快照](D:\study\study_flink\hotnews-realtime\tests\roles\sink-check\evidence\current-ranking\ranking-live-20261007-151112.json)
- [当前 MySQL/Redis 逐字段比较](D:\study\study_flink\hotnews-realtime\tests\roles\sink-check\evidence\current-ranking\ranking-live-20261007-151112-comparison.json)
- [Rule C MySQL 与独立 SQL 逐字段比较](D:\study\study_flink\hotnews-realtime\tests\roles\sql-check\evidence\20261008-day5-final\mysql-vs-sql.json)
- [Rule C MySQL 端快照](D:\study\study_flink\hotnews-realtime\tests\roles\recovery\evidence\20261008-131831-505-final-before-validation\external-snapshot.json)

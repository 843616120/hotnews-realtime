# 三条规则的逐窗口验收

Role A/B/C 的实时链路保持 `生成器 -> Kafka -> ArticleJoinBehavior -> Role`。
独立 SQL 基准只读取 `generator/generator/data/article_stream.jsonl` 和
`generator/generator/data/behavior_stream.jsonl`，不读取 Flink 的 ETL、Join 或 Role
输出。以下命令均在项目根目录执行。

## 固定输入

当前固定数据是 seed `20260927` 的文章 500 条、行为 100000 条。不要在核对期间
重新生成文件，或让 Kafka Topic 混入其他批次；发送到 Kafka 的消息必须与这两份
JSONL 一致。若需要重新生成并发送相同内容，可先备份已有文件，再执行：

```powershell
py generator\generate_data.py `
  --article-count 500 --behavior-count 100000 --seed 20260927 `
  --disorder-min 5 --disorder-max 30 --dirty-ratio 0.01 --duplicate-ratio 0.02 `
  --output both --output-dir generator\generator\data `
  --kafka-bootstrap-servers localhost:9092
```

`--output both` 会覆盖原始 JSONL，且会向 Kafka 再发送一批消息；已有旧消息时
不要把两批当作一次固定输入验收。仅需离线查看 SQL 时，不必运行生成器或 Kafka。

## 在 IDEA 中执行独立 SQL

使用 IDEA Terminal（Node 需要支持 `node:sqlite`）：

```powershell
node --no-warnings tests\roles\verify.js
node --no-warnings tests\roles\verify.js --role a --through 2026-09-27T00:59:00Z
```

`verify.js` 只负责把原始 JSONL 导入 SQLite；`prepare.sql` 在 SQLite 中独立清洗、
按 event_id 去重并关联文章与行为；`role_a.sql`、`role_b.sql`、`role_c.sql` 分别
产出带 `window_start_ms`、`window_end_ms` 的基准行。默认打印**每个窗口**的
行数，不是只打印一个最终总数。当前固定输入对应 500 条有效文章、99000 条
Schema 有效行为（其中 1956 条重复 event_id）；ETL 去重后 97044 条全部能找到
最初版本的文章，且没有行为早于首次发布时间。完整基准 A=123、B=60、C=0。
SQL 和 Java ETL 都要求 IPv4 有四段十进制数字且每段不超过 255；测试另覆盖
同一 event_id 先到的副本为脏数据、后到合法副本仍可参与 Join 的情况。
更新版文章的事件时间不应代替 version=1 publish 的首次发布时间；
早于首次发布的行为进入跨流时序 ETL 脏数据旁路，不进入 `joined` 基准。

需要在 IDEA 的 Database 工具窗口直接执行三份 SQL 时，先生成一次 SQLite 数据库
（目标文件必须尚不存在）：

```powershell
node --no-warnings tests\roles\verify.js --sqlite "$env:TEMP\hotnews-roles-baseline.sqlite"
```

在 Database 工具窗口新建 SQLite 数据源，选择刚才输出的数据库路径。打开三份
`role_*.sql` 分别执行，可按时间、文章 ID、排名或 IP 查询具体窗口结果。
`article_raw`、`behavior_raw` 是未经清洗的固定输入，`article_clean`、
`behavior_clean`、`joined` 是独立基准的中间表；`behavior_clean` 在 Schema
校验之后、Join 之前完成 `event_id` 去重。再次创建数据库时换新文件名，
脚本不会覆盖旧文件。若 IDEA 版本不提供 Database 工具，可直接用 Terminal 查看
逐窗口计数。

## 对照 Flink 日志

分别在 IDEA 运行 `ArticleJoinBehavior --bounded` 及 `Achieve_roleA/B/C --bounded`，
把控制台输出保存为独立的 UTF-8 文本文件。四个有界作业都从两条 Topic 的
最早位点回放，等待最初版本的文章到达后再处理先到的行为；不依赖消费组历史
提交位点。常驻模式从文章 Topic 最早位点重建维表，行为沿用消费组位点；
没有首次发布版本的行为立即进入 `UNMATCHED_BEHAVIOR`，待匹配状态 2 小时
到期或有界回放结束仍未关联时进入 `REPLAY_REQUIRED`，需要原始 Topic 补算。
Topic 中若有多批数据，仍会混入结果：验收前必须确认 Kafka 只保留与 JSONL
相同的一批文章/行为，或者创建隔离 Topic，不能拿跨批次日志和单批 SQL 比较。
常驻模式不传 `--bounded`，末尾窗口未必已经由 Watermark 关闭。

```powershell
node --no-warnings tests\roles\verify.js --role a --flink-log 路径\role-a.log
node --no-warnings tests\roles\verify.js --role b --flink-log 路径\role-b.log
node --no-warnings tests\roles\verify.js --role c --flink-log 路径\role-c.log
```

脚本以 A 的窗口+文章 ID、B 的窗口+排名、C 的 IP+告警分钟为键，逐窗口
打印 SQL/Flink 行数、缺失、多出和字段差异；发现差异时退出码为 1。
迟到补算造成 A/B 重复输出时取最后版本；B 重发完整的前五名。C 后到点击
可能更正最早告警的窗口边界、计数，或发出 `retracted=true` 撤销原告警；
校验器按分钟键应用更新和撤销，再检查最终窗口起止及指标。校验器按
`top_articles` 的数组顺序与文章字段比较，不受 JSON 对象的字段顺序影响。
`detect_time` 是作业执行时间，不参与比较。常驻作业可追加 `--through ISO时间`，只比较窗口结束不晚于该时间
的结果。例如旧 A 日志最后一个窗口结束于 `2026-09-27T00:59:00Z`，应使用
`--through 2026-09-27T00:59:00Z`，不能直接将 115 行与全部 123 行比较。

独立 SQL 不重演双 Topic 的真实到达顺序；新 Join 对文章与行为先做 Schema 清洗，
行为早于首次发布时进入 `DIRTY_BEHAVIOR_ETL`；先到的行为立即进入
`UNMATCHED_BEHAVIOR`，后续关联成功进入 `REJOINED_BEHAVIOR`，
有界结束仍未解决的进入 `REPLAY_REQUIRED`。要核对“所有有效行为关联成功”，
不能只看角色的最终告警数，还要检查 Join 审计总数及上述旁路。
C 在当前大样本上基准为 0：两侧均无告警不等于证明告警链路正确，
正例、迟到改早与撤销由 Java 小样本测试覆盖。

## B 的迟到补算与旧日志

旧 `role-b.log` 有 60 条窗口排名以及 4097 条 `ROLE_B_LATE_INPUT`；仅首窗口
就有 2237 条 science 和 1815 条 world 被第一阶段丢弃。首窗口 SQL 的
science=5455，而旧 Flink science=439、world=3298：不是取最后一行
`rank=1` 得出的结论。最后一行属于另一十分钟窗口。
此外旧日志的全部窗口前五名累计分数为 32067，明显低于独立 SQL 的 97044 条
Join 候选；第一阶段旁路不能解释全部缺口。旧日志没有 Join 层的旁路明细，
故不能单凭该日志将余量精确归因给 Join、消费位点或其他输入问题。

新的 B 使用事件时间按文章保存十分钟累计值，窗口关闭后晚到的行为更新原值，
按窗口键覆盖旧文章得分；每五秒水位线合并一轮修正并输出整份排名。
窗口结束 24 小时后用事件时间定时器清理两级状态；超过该边界分别进入 `ROLE_B_LATE_INPUT` /
`ROLE_B_LATE`，不能声称已自动补算。此保留期为回放和持续运行状态的上限，
并非把 30 秒 `allowedLateness` 简单改大；状态在水位线不推进时也不会自动清理。
Join 的两条流在 ETL 后分别生成 Watermark（文章 30 秒，行为常驻 65 分钟；
有界跨分区全量回放按实际两小时时间跨度等待 2 小时），超出 Join 的
30 秒允许迟到会输出 `LATE_DATA`，仍继续关联。缺少首版文章时，
持续模式必须保留等待状态，
需要监控其大小并补齐文章 Topic；无法对实际不存在的文章承诺必然关联成功。
Role A/C 也采用按事件时间保留 24 小时的迟到修正状态；超过边界的点击仍进入
各自的 `ROLE_A_LATE` / `ROLE_C_LATE`，不能无限期保证补算。
旧日志不会被新代码自动改写，必须重新运行作业生成新日志才能验证。

旧版 `join-recheck.log` 与 `role-{a,b,c}-recheck.log` 是 Join 后去重的旧口径，
不能再作为当前验收证据。本轮有界回放保存在 `join-etl-dedup-20261005.log`、
`role-{a,b,c}-etl-dedup-20261005.log`：Join 输出 97044、Schema 脏行为
1000、ETL 重复旁路 1956、Join 迟到和待回放 0；A 最终 123 行、B 最终
60 行逐窗口与 SQL 相符，C 和 SQL 同为 0 行（正例见单测）。
新增 `role-b-snapshot-20261005.log` 在 B 携带完整 Top 5 快照后再次
逐窗口验证 60 行，全部字段差异为 0。第五天外部 Sink 的验收步骤见
`docs/04-Day5-Sink与恢复验收.md`；后续 Docker/Flink 现场演练见
`tests/day5/README.md`，其中未完成的 MySQL 验证单独标注。
修正中间打印数量会因水位线
推进时机改变，不作为验收指标。B 首窗口最终 science=5455、world=5432。
本机从打包的 JAR 运行时需显式指定 UTF-8 和 `slf4j-api`，否则中文标题可能因
Windows 控制台编码被写坏；在 IDEA 的 UTF-8 运行配置里不必复用这段类路径：

```powershell
mvn -o -f flink-job/pom.xml -pl flink-rolesachieve -am package -DskipTests
$jar = "flink-job/flink-rolesachieve/target/flink-rolesachieve-1.0-SNAPSHOT-all.jar"
$slf4j = "$env:USERPROFILE/.m2/repository/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
$log = Join-Path $env:TEMP ("role-b-" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".log")
java '-Dfile.encoding=UTF-8' -cp "$jar;$slf4j" Achieve_roleB --bounded *> $log
node --no-warnings tests/roles/verify.js --role b --flink-log $log
```

同样用 `$jar;$slf4j` 分别运行 `Achieve_roleA --bounded`、
`Achieve_roleC --bounded`；Join 审计将 `$jar` 换为
`flink-job/flink-join/target/flink-join-1.0-SNAPSHOT-all.jar`，
执行 `ArticleJoinBehavior --bounded` 并检查 `JOINED_TOTAL`、
`DIRTY_BEHAVIOR_ETL`、`DUPLICATE_BEHAVIOR`、`UNMATCHED_BEHAVIOR`、
`REPLAY_REQUIRED` 与 `LATE_DATA`。

## 回归测试

第四天的热点倾斜测试、状态 TTL 与类名解释见
[`../day4/README.md`](../day4/README.md) 和
[`../../docs/03-状态TTL与内存模型笔记.md`](../../docs/03-状态TTL与内存模型笔记.md)。
规则 C 的更新包含 `alert_minute`，撤销行包含 `retracted=true`；独立
`IpWindowStateJob` 的输出包含
`article_ids[]`，但它使用一分钟不重叠窗口，不应拿本目录的逐点击回看
SQL 直接核对。

```powershell
node --no-warnings --test tests\roles\baseline.test.js
mvn -o -f flink-job/pom.xml -pl flink-rolesachieve -am test
```

Node 测试使用单独的固定小样本，验证清洗、去重、三个 SQL 的窗口边界及
逐窗口差异检测、SQL 首次发布下界、B 的字段顺序和 C 的撤销应用；
并验证 SQL 在 Join 前拒绝非法 IP、优先保留 Schema 合法的重复行为；
Java 测试覆盖 Join 首版晚到/脏行为、A 关窗后跨阈值、B 名次反转和
C 迟到改早/撤销。它们不依赖 Kafka，不会重写生成器数据。
根目录 `pom.xml` 同时包含父子模块，会触发 Maven 重复模块报错，因此从
`flink-job/pom.xml` 运行。

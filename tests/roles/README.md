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
有效行为、97044 条离线 Join 候选；完整基准 A=123、B=60、C=0。

需要在 IDEA 的 Database 工具窗口直接执行三份 SQL 时，先生成一次 SQLite 数据库
（目标文件必须尚不存在）：

```powershell
node --no-warnings tests\roles\verify.js --sqlite "$env:TEMP\hotnews-roles-baseline.sqlite"
```

在 Database 工具窗口新建 SQLite 数据源，选择刚才输出的数据库路径。打开三份
`role_*.sql` 分别执行，可按时间、文章 ID、排名或 IP 查询具体窗口结果。
`article_raw`、`behavior_raw` 是未经清洗的固定输入，`article_clean`、
`behavior_clean`、`joined` 是独立基准的中间表。再次创建数据库时换新文件名，
脚本不会覆盖旧文件。若 IDEA 版本不提供 Database 工具，可直接用 Terminal 查看
逐窗口计数。

## 对照 Flink 日志

分别在 IDEA 运行 `Achieve_roleA`、`Achieve_roleB`、`Achieve_roleC`，把每条规则
的控制台输出保存为独立的 UTF-8 文本文件。使用 `--bounded` 验收时，作业从其
消费组位点读到启动时的 Topic 末尾并退出；已有提交位点可能跳过旧消息，
旧 Topic 数据也可能混入结果，验收前要核实 Kafka 的批次与消费组位点。
常驻模式不传 `--bounded`，末尾窗口未必已经由 Watermark 关闭。

```powershell
node --no-warnings tests\roles\verify.js --role a --flink-log 路径\role-a.log
node --no-warnings tests\roles\verify.js --role b --flink-log 路径\role-b.log
node --no-warnings tests\roles\verify.js --role c --flink-log 路径\role-c.log
```

脚本以窗口边界及 A 的文章 ID、B 的排名、C 的 IP 为键，逐窗口打印 SQL/Flink
行数、缺失、多出和字段差异，并列出具体差异；发现差异时退出码为 1。迟到补算
造成同一窗口多次打印时，以日志中的最后一次为准；`detect_time` 是作业执行时间，
不参与比较。常驻作业可追加 `--through ISO时间`，只比较窗口结束不晚于该时间
的结果。例如旧 A 日志最后一个窗口结束于 `2026-09-27T00:59:00Z`，应使用
`--through 2026-09-27T00:59:00Z`，不能直接将 115 行与全部 123 行比较。

独立 SQL 是固定输入的业务候选基准，不能重演双 Topic 的真实到达顺序、Join
状态超时及 Watermark 丢弃的迟到数据；这些差异需要结合 Join 的旁路和实际输入
排查。C 在当前大样本上基准为 0：两侧都没有告警不等于证明告警链路正确，
需另用能触发阈值的固定输入及 Flink 日志验收正例。

## 回归测试

第四天的热点倾斜测试、状态 TTL 与类名解释见
[`../day4/README.md`](../day4/README.md) 和
[`../../docs/03-状态TTL与内存模型笔记.md`](../../docs/03-状态TTL与内存模型笔记.md)。
原规则 C 的日志格式保持不变；新 `IpWindowStateJob` 的输出包含
`article_ids[]`，但它使用一分钟不重叠窗口，不应拿本目录的逐点击回看
SQL 直接核对。

```powershell
node --no-warnings --test tests\roles\baseline.test.js
mvn -o -pl flink-job/flink-rolesachieve -am test
```

Node 测试使用单独的固定小样本，验证清洗、去重、三个 SQL 的窗口边界及
逐窗口差异检测；Java 测试覆盖规则阈值。它们不依赖 Kafka，不会重写生成器数据。

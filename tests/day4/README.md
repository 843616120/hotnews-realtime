# 第四天：固定输入与验收

从项目根目录运行，Java 8 / Flink 1.17，均不依赖 Kafka：

```powershell
mvn -o -pl flink-job/flink-rolesachieve -am test
node --no-warnings --test tests\roles\baseline.test.js
mvn -o -pl flink-job/flink-rolesachieve -am '-Dtest=Day4SkewBenchmark' '-Dsurefire.failIfNoSpecifiedTests=false' '-Dday4.backend=hashmap' test
```

第一条检查去重 TTL（测试时缩短为 100 ms）、原规则 C 与独立 IP 作业的
49/50/51、1999/2000 ms、重复事件、文章规则的 999/1000/1001 和分钟边界；
同一批事件分别比较两个独立作业的原始模式与加盐模式。
第二条检查原有独立 SQL 基准。第三条是显式运行的本地性能实验，默认测试不会跑。

报告保存在 `tests/day4/results/local-skew-*.json`，每次运行创建新文件；
不覆盖上一轮数据。文章和 IP 各有 12000 条输入，其中 10800 条（90%）
分别落在一篇文章/一个 IP，并行度 3，各自预热后交错测两轮。
`p95_source_to_keyed_ms` 是 Source
打点至 keyed 探针的排队/传输耗时，**不是告警端到端时延**；
`events_per_second` 包含本地作业启动与结束开销，**不是持续生产吞吐上限**。
`article_heat` 和 `ip_window` 各有四份结果和 `window_results_equal`。

在 IDEA 中分别启动 `ArticleHeatStateJob`、`IpWindowStateJob`：
不传参数为常驻原始模式，`--optimized` 为常驻加盐模式；追加 `--bounded`
有界读取。两者还接受 `--backend=hashmap|rocksdb`，默认 hashmap。
在 IDE 运行 RocksDB 需为 Flink 1.17 添加对应 `flink-statebackend-rocksdb`
依赖，集群运行则需对应版本后端 JAR。独立消费组之间要确保 Topic 批次和
起始位点相同，保存 UTF-8 日志；
各作业的两种模式逐窗口比较最终输出。文章作业沿用 A 的五分钟滑动窗口，
可另对照 `tests/roles/role_a.sql`。IP 状态作业采用一分钟**不重叠**窗口，
原规则 C 使用“每次点击回看一分钟”，不能用 C 的逐点击 SQL 对照新作业。
原规则 C 的大样本 SQL 基准若为 0，还需本目录的 Java 正例断言验收阈值。
Docker Linux 资源采集从项目根目录执行（脚本会创建唯一 `--run-id`，避免旧消费位点污染样本）：

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload article -Backend hashmap
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload article -Backend hashmap -Optimized
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload article -Backend rocksdb
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload article -Backend rocksdb -Optimized
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload ip -Backend hashmap
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload ip -Backend hashmap -Optimized
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload ip -Backend rocksdb
pwsh -NoProfile -ExecutionPolicy Bypass -File tests/day4/run-docker-day4.ps1 -Workload ip -Backend rocksdb -Optimized
```

脚本只有在 `input_behavior_records=100000`、`valid_full_batch=true`、`had_restart=false` 时才可作为有效样本；半批样本不要写入报告。`--run-id` 可手动传入唯一后缀，默认由脚本自动生成。资源对照请保持相同输入批次、并行度和硬件，分别记录四组运行的
TaskManager 堆/托管内存、RocksDB 本地磁盘、GC、Checkpoint 大小和时长、
吞吐与 p95。具备依赖时，基准入口可执行：

```powershell
mvn -pl flink-job/flink-rolesachieve -am -Pday4-rocksdb '-Dtest=Day4SkewBenchmark' '-Dsurefire.failIfNoSpecifiedTests=false' '-Dday4.backend=rocksdb' test
```

对照 `local-skew-hashmap-*.json` 与 `local-skew-rocksdb-*.json` 中同一
`article_heat` / `ip_window` 的输出及吞吐 p95，并同时采集上述资源指标。
Windows 本地 JNI 可能失败（`librocksdbjni-win64.dll: Can't find dependent libraries`），
应使用 Linux Docker 运行 RocksDB。正式 Docker 证据保存在 `tests/day4/evidence/`；
本次采集的 8 组有效样本已写入 `docs/05-第四天状态与倾斜验收报告.md`。
正式 Docker 作业没有 Source-to-keyed Probe，因此其 p95 字段为空，不能用
Checkpoint 或 backpressure 指标代替 p95。

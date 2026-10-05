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
资源对照请保持相同输入批次、并行度和硬件，分别记录四组运行的
TaskManager 堆/托管内存、RocksDB 本地磁盘、GC、Checkpoint 大小和时长、
吞吐与 p95。具备依赖时，基准入口可执行：

```powershell
mvn -pl flink-job/flink-rolesachieve -am -Pday4-rocksdb '-Dtest=Day4SkewBenchmark' '-Dsurefire.failIfNoSpecifiedTests=false' '-Dday4.backend=rocksdb' test
```

对照 `local-skew-hashmap-*.json` 与 `local-skew-rocksdb-*.json` 中同一
`article_heat` / `ip_window` 的输出及吞吐 p95，并同时采集上述资源指标。
本机没有可用 Docker；RocksDB 依赖下载后本地 Windows JNI 加载失败
（`librocksdbjni-win64.dll: Can't find dependent libraries`）。
因此集群和 RocksDB 资源对照未实跑，不能把失败记录填成性能数据。

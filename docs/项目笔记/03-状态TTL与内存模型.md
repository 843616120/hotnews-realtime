# 03 状态 TTL 与内存模型

## 状态选择与生命周期

`ValueState` 适合一个 Key 的最新文章、首次发布时间、去重标记和最近热度；`MapState` 按 ID/窗口保存待关联行为、分片累计值或 IP 点击/告警版本；`ListState` 适合需要顺序保留的多个值，本项目核心路径没有因为考核提到它就强行使用。Keyed State 的 Key 决定分布：同一 `article_id` 或 IP 始终落在同一子任务，热点会集中 CPU 与状态访问。

| 状态 | 实现与清理 | 口径 |
| --- | --- | --- |
| 行为 `event_id` 去重 | [`ArticleJoinBehavior`](../../flink-job/flink-join/src/main/java/ArticleJoinBehavior.java) 的 `ValueState`，处理时间 TTL 24h | 先清洗再去重；24h 后不能承诺永久唯一 |
| 文章 Join/首版 | `ValueState`，处理时间 TTL 2h | 首次发布确定行为有效下界；待匹配行为另有显式定时器/旁路 |
| 规则 A/B/C 修正 | Keyed `MapState` 与事件时间清理定时器，窗口结束后最多 24h | Watermark 推进才触发；停更会滞留 |
| Day 4 最近文章热度 | [`ArticleHeatStateJob`](../../flink-job/flink-rolesachieve/src/main/java/ArticleHeatStateJob.java) 的 `ValueState`，处理时间 TTL 24h | 只保存最近已输出的热点结果 |
| Day 4 IP 窗口/分片 | [`IpWindowStateJob`](../../flink-job/flink-rolesachieve/src/main/java/IpWindowStateJob.java) 的状态，处理时间 TTL 1h | 不同文章集合、点击数、时长总和、起止时刻；一分钟不重叠，不替代规则 C |

TTL 是失效边界，不是窗口触发器，也不保证达到期限时立刻物理清理。处理时间 TTL 不能当事件时间的补算承诺。文章分片的窗口局部状态在窗口结束加 30 秒后由定时器合并清理；IP 分片先局部累计、再按 IP 合并，保存最近窗口及告警版本。字段和清理依据详见 [既有状态笔记](../03-状态TTL与内存模型笔记.md)。

## 容量估算与后端选择

估算键状态占用：`唯一事件速率 × 保留秒数 × 单键字节数`。固定数据按每小时 10 万行为、24h、80 B/标记，约 192 MB；若持续 2000/s 且全部为新 ID，约 1.728 亿键、13.8 GB（均**不含**对象、索引、序列化和 Checkpoint 开销）。同一 IP 在一分钟内出现 12 万不同文章且单 ID 集合开销按 80 B，则至少约 9.6 MB/窗口。真实工作集必须用 TaskManager JVM/托管内存、GC、后端磁盘、状态快照大小和峰值重复率复核。

HashMap 状态主要占 JVM 堆，访问直接，但大状态可能引发 GC/OOM 和大快照；RocksDB 将 keyed state 放在本地磁盘并占用托管内存，可能缓解堆压力，代价是 JNI、序列化、I/O 与 compaction。只有相同输入、硬件、并行度和配置下比较成功快照、延迟与结果，才可判断是否切换；`state.backend.rocksdb.localdir` 的配置本身不证明运行作业已选 RocksDB。

## 倾斜实验、结果与问题

本地 [Day 4 报告](../05-第四天状态与倾斜验收报告.md)每组 12000 条、90% 热点、并行度 3；按 `event_id` 稳定哈希分 16 片再二次合并。文章原始 480/11160/360，优化后 5681/2426/3893；IP 原始 11520/360/120，优化后 3796/3759/4445。两组最终窗口结果一致，本地吞吐没有稳定提高；Source 到 keyed 探针 p95 **不是**告警端到端延迟。规则 C 的 49/50/51 篇与 1999/2000 ms 边界由 Java 测试核对。

10 月 6 日的 [Docker Day 4 原始记录](../../tests/day4/evidence/)已有文章和 IP 的 HashMap/RocksDB、原始/加盐共 **8 组有效完整批次**（每组 100000 行为、无重启）。文章 HashMap 基线/加盐约 10385/7276 events/s，RocksDB 基线/加盐约 934/679 events/s；IP HashMap 约 6478/9660，RocksDB 约 1156/1418。两组 RocksDB 加盐各有 1 次 Checkpoint 失败，最终仍完成；详细内存、后端目录和 Checkpoint 表见 [现场报告](../05-第四天状态与倾斜验收报告.md)。原始目录也有仅消费 0/4216/10219 条的历史样本，**不得与 8 组有效数据混算**。正式作业的 `p95_source_to_keyed_ms` 均为 `null`；本地前后窗口断言不能冒充 Docker 作业的逐字段输出对比。Windows JNI 失败是本地平台限制，不再意味着 RocksDB 没有集群实测。

**结论：**分片可改善热点分布，但引入第二阶段成本；HashMap 与 RocksDB 的取舍依赖状态规模和可靠性/延迟实测，不以单次吞吐决定。更详细的 TTL 推导、局限和已采指标留在原有 Markdown 记录中。

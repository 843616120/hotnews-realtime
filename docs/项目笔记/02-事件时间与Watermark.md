# 02 事件时间与 Watermark

## 概念与传播

处理时间跟随算子执行时钟；事件时间由 `event_time` 决定，乱序到达仍应归入原本的业务窗口。Watermark 表示当前算子预计不会再收到更早事件的进度；多 Kafka 分区和双流合并时，下游取仍活跃输入的最小水位线。停更分区可使窗口长期不关，`withIdleness` 让它在空闲后不阻塞其他分区。窗口边界是 `[start,end)`，Watermark 推进到对应边界才有最终触发条件；连续流整体停更不能凭墙上时钟宣布最后一个窗口已完成。

窗口 API 的 `allowedLateness` 会在首次触发后保留窗口状态并允许再次触发；本项目**双流 Join 使用手写定时器与 `LATE_DATA` Side Output，不是在 Join 上调用窗口 API**。独立 `ArticleHeatStateJob` 的优化窗口使用 `.allowedLateness(30s)`；A/B/C 主规则使用可修正的 keyed 状态和事件时间定时器，三者不能混写为同一种 API。

## 项目实现与边界

[`ArticleJoinBehavior`](../../flink-job/flink-join/src/main/java/ArticleJoinBehavior.java)先解析 JSON/校验 Schema，把不合法原文送 `dirty_data`；合法行为在 **Join 前**按 `event_id` 去重，才分配文章/行为各自的 Watermark。文章容忍 30 秒乱序；行为持续作业配置 65 分钟，有界跨分区重放 2 小时；空闲检测 30 秒。行为等待值覆盖固定跨分区回放的到达跨度，不是生成器每条记录 5-30 秒扰动的简单复制，等待越长，窗口输出可能越晚。

文章状态以 `article_id` 保存最近版本和最初 `version=1` 发布时刻，处理时间 TTL 2 小时。行为先到立即进入 `UNMATCHED_BEHAVIOR`，同时以 `event_id` 缓存在待关联 MapState；首次发布到达且行为时间不早于首次发布时，补 Join 并富化。早于首次发布则进 `DIRTY_BEHAVIOR_ETL`。等待状态过期或有界回放结束仍无首版，进入 `REPLAY_REQUIRED`，应在 Kafka 保留期内隔离重放，原文章根本不存在时不能承诺补齐。

记录早于当前 Watermark 减 30 秒时向 `LATE_DATA` 留痕，但仍继续尝试关联；规则 A/B/C 的事件时间修订状态最多保留窗口结束后 24 小时，超期分别留在 `ROLE_A_LATE`、`ROLE_B_LATE_INPUT/ROLE_B_LATE`、`ROLE_C_LATE`。异常入表不等于自动离线补算，原始 Schema 和重复旁路主要要看日志。文章热点优化实验仅在自身窗口使用 30 秒允许迟到。

## 实验、问题与结论

[Day 2 固定输入](../../tests/day2/README.md)的离线 SQLite 基线：50 篇文章、1000 条行为、10 条脏行为、20 个重复事件 ID、990 条清洗后 Join 候选。小样本覆盖非法 JSON、空键、负时长、未来事件、先到行为与晚到文章。**候选集合不是实时 Join 输出量**，分区推进/停顿会影响迟到判断；用 `verify.py --flink-log` 比较 `dirty_missing`、`joined_wrong_article_fields`、`late_actual` 和 `unmatched_actual`，而非只比总数。

固定 10 万条数据的 [独立 SQL](../../tests/roles/README.md)先清洗再去重，最终 97044 条可关联；历史有界回放 Join 日志对应 97044。窗口 A/B 分别 123/60 行，C=0；持续流末尾未关闭的窗口不能直接和有界结果比较。现有材料仍缺完整在线空闲分区与迟到边界的逐事件截图/日志组合，应在隔离 Topic 下补录真实 Watermark 值、输入分区和旁路输出。维表异步 I/O/外部缓存不是当前 Join 的实现：本项目用本地文章 keyed state 富化；若维度规模超过内存/TTL 保留范围，再评估缓存失效、一致性、异步容量与超时。

**结论：**事件时间使可等待和可补算范围内的乱序记录归入正确业务窗口，但超出保留边界仍需旁路或离线补算，且乱序等待会增加输出延迟和状态成本。漏掉首次发布或 Kafka 保留期不足时，不能用“配置了 Watermark”代替补偿设计。

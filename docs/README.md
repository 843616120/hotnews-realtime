# 学习记录和验收材料

这里保存项目架构、数据字典、Watermark、Join 状态、TTL、Checkpoint、反压压测、故障恢复 SOP、测试报告和代码审计记录。
第五天可执行步骤、类名思路和外部一致性边界见
[04-Day5-Sink与恢复验收.md](04-Day5-Sink与恢复验收.md)。
一致性边界的一页时序图见
[05-Day5-一致性边界.md](05-Day5-一致性边界.md)，
真实故障/Savepoint 记录见 [../tests/day5/README.md](../tests/day5/README.md)。

## Word 笔记

- [01-项目架构与数据字典.docx](01-项目架构与数据字典.docx)
- [02-事件时间与Watermark笔记.docx](02-事件时间与Watermark笔记.docx)
- [03-状态TTL与内存模型笔记.docx](03-状态TTL与内存模型笔记.docx)
- [04-Checkpoint与一致性笔记.docx](04-Checkpoint与一致性笔记.docx)
- [05-反压压测与容量规划.docx](05-反压压测与容量规划.docx)
- [06-故障演练与恢复SOP.docx](06-故障演练与恢复SOP.docx)
- [07-版本源码与代码审计.docx](07-版本源码与代码审计.docx)
- [test-report.docx](test-report.docx)

文档中的结果分为“已运行核对”和“待实测”两类。当前已记录 Day 2 两组 SQLite 离线基线，以及 Day 4 固定热点输入的本地基准；Kafka 在线输出、MySQL/Redis Sink、持续压测和故障恢复仍需按各文档的步骤补充证据。

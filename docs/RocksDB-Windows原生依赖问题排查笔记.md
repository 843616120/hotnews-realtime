# RocksDB Windows 原生依赖问题排查笔记

## 概念说明

Flink 的 RocksDB 状态后端不是纯 Java 实现。Flink Java 代码通过 JNI 调用 RocksDB 的本地 DLL，Windows 必须同时满足以下条件：

- Flink 状态后端依赖与 Flink 版本匹配；
- JVM、操作系统和 RocksDB DLL 的位数一致，当前项目使用 64 位 Windows 和 64 位 JVM；
- RocksDB DLL 依赖的 C/C++ 运行库已经安装；
- JNI 包中的原生 DLL 必须是可运行的 Release 构建。

因此，`Could not load the native RocksDB library` 不一定表示 Java 代码或 Checkpoint 配置错误。需要继续查看最底层的 `UnsatisfiedLinkError`，并检查 DLL 的实际依赖。

## 项目中的具体实现

统一作业是 `com.agd.flink.roles.roleall.Achieve_roleAll`。为了比较两种状态后端，作业增加了环境变量选择：

- 未设置 `HOTNEWS_STATE_BACKEND` 或设置为 `hashmap`：使用 `HashMapStateBackend`；
- 设置 `HOTNEWS_STATE_BACKEND=rocksdb`：使用 `EmbeddedRocksDBStateBackend(true)`；
- Join、Watermark、窗口、并行度、Checkpoint 和 MySQL/Redis Sink 均不随状态后端切换。

RocksDB 依赖位于 `flink-job/flink-rules/pom.xml`。Flink 1.17 的状态后端模块仍然使用 `flink-statebackend-rocksdb`，但显式固定 JNI 版本为：

```xml
<dependency>
    <groupId>com.ververica</groupId>
    <artifactId>frocksdbjni</artifactId>
    <version>6.20.3-ververica-1.0</version>
</dependency>
```

IDEA 的统一作业运行配置使用独立消费组 `state-compare-rocksdb-20261007`、独立日志 `tests/roles/state-backend/logs/state-rocksdb-20261007.log` 和 Web UI 端口 `10011`，避免把 HashMap 基线和 RocksDB 运行数据混在一起。

## 测试结果

### 第一次失败现象

RocksDB 作业能够编译并进入 Flink 作业提交流程，但在创建第一个带 Keyed State 的算子时失败。Web UI 没有形成可用作业，端口 `10011` 随后无法访问。日志位置：

`tests/roles/state-backend/logs/state-rocksdb-20261007.log`

最底层异常为：

```text
Could not load the native RocksDB library
java.lang.UnsatisfiedLinkError:
librocksdbjni-win64.dll: Can't find dependent libraries
```

### 排查过程

1. 检查了 VC++ 运行库，系统中已经存在 `vcruntime140.dll`、`vcruntime140_1.dll`、`msvcp140.dll` 和 `ucrtbase.dll`。
2. 检查了 JVM 和操作系统位数，均为 64 位；因此不是 32/64 位不匹配。
3. 解析 `frocksdbjni-6.20.3-ververica-2.0.jar` 中的 `librocksdbjni-win64.dll` 的 PE 导入表，发现它依赖：

```text
MSVCP140D.dll
VCRUNTIME140D.dll
VCRUNTIME140_1D.dll
ucrtbased.dll
```

这些带 `D` 的文件是 Visual Studio Debug 运行库，普通的 Microsoft Visual C++ 2015-2022 Redistributable x64 不会提供它们。因此，继续安装普通 VC++ 运行库不能解决该问题。

4. 对同一 JNI 版本的 Release 包进行检查，`frocksdbjni-6.20.3-ververica-1.0` 的 DLL 依赖普通 Release 运行库：

```text
MSVCP140.dll
VCRUNTIME140.dll
api-ms-win-crt-*.dll
```

### 修复验证

- 已将项目显式依赖切换到 `frocksdbjni 6.20.3-ververica-1.0`；
- 已移除不存在的 `flink-state` 模块声明，恢复完整 Maven reactor 编译；
- `mvn -pl flink-job/flink-rules -am package -DskipTests` 编译通过；
- Maven 打包日志确认最终使用 `com.ververica:frocksdbjni:6.20.3-ververica-1.0`；
- 修复前的作业失败发生在状态后端初始化阶段，尚未产生业务输出、Checkpoint 或性能指标，因此这次失败数据不纳入 HashMap/RocksDB 性能对比。

### 修复后的运行验证

重新启动后的 Job ID 为 `35dd0a34687b84007ca056a72749aa8e`，Web UI `http://localhost:10011` 显示作业进入 `RUNNING`，日志不再出现 `UnsatisfiedLinkError`，说明 Release JNI 依赖已经解决了启动问题。

后续运行中 Checkpoint #1、#2、#3 成功，分别耗时约 39.8 秒、150.9 秒和 179.7 秒；Checkpoint #4、#5、#6 分别在约 180 秒超时。由此可以区分两个问题：JNI 原生库加载问题已经解决，但 RocksDB 状态增长、下游反压和当前单机 IO 条件仍造成 Checkpoint 超时风险。此次运行数据已补充到 `docs/03-状态TTL与内存模型笔记.md`，不能把 Checkpoint 超时误判为 JNI 依赖错误。

## 问题和结论

本次问题根因是 Flink 1.17 默认传递的 `frocksdbjni 6.20.3-ververica-2.0` Windows 原生 DLL 使用 Debug 运行库，而本机只安装了 Release 版 VC++ 运行库。错误虽然表现为“找不到依赖库”，但缺少的不是普通 VC++ 运行库，而是 Debug 运行库或对应的可运行 Release JNI 包。

最终采用显式固定 `frocksdbjni 6.20.3-ververica-1.0` 的方式解决，保留 Flink 1.17 的 RocksDB 状态后端和项目业务代码不变。该方案已经通过 Maven 编译，但在修复依赖后的作业重新运行完成前，不能声称 RocksDB 运行时指标、Checkpoint 和 HashMap 对比已经验收通过。

后续验证顺序：

1. 停止失败的旧 IDEA 作业，重新编译并启动统一作业；
2. 在 Web UI 确认状态后端为 RocksDB，作业状态为 `RUNNING`；
3. 记录 RocksDB 的 Checkpoint、Heap、Direct Memory、Kafka Lag、Join 子任务分布和反压；
4. 与 HashMap 基线使用相同输入批次和指标口径进行对比。

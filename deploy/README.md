# 本地运行环境

这个目录使用 Docker Compose 启动本地学习环境，包含：

- Zookeeper：Kafka 的协调服务
- Kafka：文章流和行为流的消息队列
- MySQL：结果明细和维度数据存储
- Redis：热点榜单存储
- RedisInsight：Redis 图形管理界面
- Flink JobManager：Flink 作业管理节点
- Flink TaskManager：Flink 作业执行节点

## 启动

第一次使用时，在项目根目录执行：

```powershell
Copy-Item deploy\.env.example deploy\.env
docker compose --env-file deploy\.env -f deploy\docker-compose.yml config
docker compose --env-file deploy\.env -f deploy\docker-compose.yml up -d
```

`config` 命令只检查 Compose 配置，不会启动服务。确认没有错误后，再执行 `up -d`。

只启动 Kafka 所需服务：

```powershell
Copy-Item deploy\.env.example deploy\.env
docker compose --env-file deploy\.env -f deploy\docker-compose.yml up -d zookeeper kafka
powershell -ExecutionPolicy Bypass -File deploy\create-kafka-topics.ps1
```

在 Windows 上也可以双击 `deploy\start-kafka.cmd` 一键启动 ZooKeeper 和 Kafka；
从项目根目录的 CMD 运行时输入 `deploy\start-kafka.cmd` 即可。脚本会在首次运行时创建
`deploy\.env`，需要先启动 Docker Desktop。该脚本仅启动服务，不创建 Topic。

运行上述 `create-kafka-topics.ps1` 后会创建以下 Topic：

```text
topic_article   partitions=3   replication-factor=1
topic_behavior  partitions=6   replication-factor=1
```

宿主机上的 IDEA/Flink 使用 `localhost:9092`，Compose 容器之间使用 `kafka:29092`。

## Redis 图形管理界面

Redis 已经启动时，只需新增 RedisInsight 容器。在项目根目录执行：

```powershell
docker compose --env-file deploy\.env -f deploy\docker-compose.yml up -d redisinsight
```

打开 <http://localhost:5540>，首次进入后选择添加数据库，填写：

- 数据库名称：`hotnews`
- Host：`redis`（Compose 服务名，页面后台连接的是容器网络）
- Port：`6379`
- 用户名和密码：留空（当前 Redis 未配置认证）

进入 Browser，搜索 `hotnews:top5:latest`，查看 Hash 的 `ranking` 字段和 TTL。
`ranking` 是完整 Top 5 JSON，包含每个分类的得分和 `top_articles[]`；Rule B
每次成功更新时将 TTL 重置为 7200 秒。Key 不存在时，先确认 Rule B 已输出
至少一个窗口榜单；停止更新两小时后，Key 会自动过期。

`redisinsight-data` 保存界面的连接配置；如需改页面端口，在 `deploy/.env`
设置 `REDISINSIGHT_PORT` 后重新启动该服务。

## Flink Web UI 与 Checkpoint

项目同时支持两种监控方式：

```text
IDEA 本地 A/B/C/统一作业: http://localhost:8083 / 8084 / 10013 / 8086
Docker JobManager:  http://localhost:8081
```

IDEA 本地运行的作业通过 `flink-runtime-web` 创建 MiniCluster Web UI。Docker 运行时，`jobmanager` 和 `taskmanager` 挂载 `flink-conf.yaml`，并共享 D 盘上的以下目录：

```text
D:\docker_data\flink\checkpoints
D:\docker_data\flink\savepoints
```

Compose 将这两个 Windows 目录分别绑定到容器内的 `/opt/flink/checkpoints` 和 `/opt/flink/savepoints`。`D:\docker_data\docker-desktop\DockerDesktopWSL` 是 Docker Desktop 自己管理的内部存储目录，不要直接作为 Flink 挂载源目录。

配置包含：

- `execution.checkpointing.interval: 10s`
- `execution.checkpointing.mode: EXACTLY_ONCE`
- `state.checkpoints.dir: file:///opt/flink/checkpoints`
- `state.savepoints.dir: file:///opt/flink/savepoints`
- `taskmanager.numberOfTaskSlots: 8`

Docker 内提交的作业必须把 Kafka 地址写成 `kafka:29092`；在 IDEA 或 PyCharm 宿主机进程中必须写成 `localhost:9092`。

## 查看状态和日志

```powershell
docker compose --env-file deploy\.env -f deploy\docker-compose.yml ps
docker compose --env-file deploy\.env -f deploy\docker-compose.yml logs -f
docker compose --env-file deploy\.env -f deploy\docker-compose.yml logs -f kafka
```

按 `Ctrl+C` 退出日志查看，不会停止容器。

## 停止和清理

```powershell
# 停止容器，保留数据
docker compose --env-file deploy\.env -f deploy\docker-compose.yml stop

# 删除容器，保留命名数据卷
docker compose --env-file deploy\.env -f deploy\docker-compose.yml down

```

MySQL 初始化脚本只在数据卷首次创建时执行。已有数据卷需要手动执行新增 SQL；不要为了更新表结构删除数据卷。

## 地址

宿主机程序连接：

```text
Kafka:  localhost:9092
MySQL:  localhost:${MYSQL_PORT}（默认 3306；本机现有实例可配置为 3307）
Redis:  localhost:6379
Flink:  http://localhost:8081
```

Compose 容器之间连接：

```text
Kafka:      kafka:29092
MySQL:      mysql:3306
Redis:      redis:6379
JobManager: jobmanager:8081
```

一体化作业位于 `flink-rules` 模块，入口为
`com.agd.flink.roles.roleall.Achieve_roleAll`。容器中的连接地址由 Compose
环境变量提供；在 IDEA 本地运行则使用宿主机端口。建表见
[`../sql/README.md`](../sql/README.md)。

## 常见概念

- 镜像：服务的可运行模板，例如 `mysql:8.0.16`。
- 容器：镜像启动后的运行实例。
- 服务：`docker-compose.yml` 中的一个容器定义。
- 数据卷：容器外部保存的数据位置，容器删除后数据仍可保留。
- 端口映射：例如 `9092:9092`，左边是 Windows 主机端口，右边是容器端口。
- `up -d`：后台创建并启动服务。
- `logs -f`：持续查看日志。
- `down`：删除 Compose 创建的容器和网络。

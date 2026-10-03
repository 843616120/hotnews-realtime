# Docker 管理脚本集合使用说明

## 快速启动脚本

### 1. 启动 Kafka 和 ZooKeeper
```powershell
.\deploy\start-kafka.ps1
```
只启动 Kafka 和 ZooKeeper，适合只需要测试 Kafka 的场景。

### 2. 停止 Kafka 和 ZooKeeper
```powershell
.\deploy\stop-kafka.ps1
```
停止 Kafka 和 ZooKeeper，但保留数据。

### 3. 启动所有服务
```powershell
.\deploy\start-all.ps1
```
启动 Kafka、MySQL、Redis、Flink 等所有服务。

### 4. 停止所有服务
```powershell
.\deploy\stop-all.ps1
```
停止所有服务，但保留数据。

### 5. 查看服务状态
```powershell
.\deploy\status.ps1
```
查看服务运行状态，并可选择查看日志。

### 6. 创建 Kafka Topics
```powershell
.\deploy\create-kafka-topics.ps1
```
创建 topic_article 和 topic_behavior 两个 Topic。

## 常用组合

### 第一次使用
```powershell
# 1. 启动 Kafka
.\deploy\start-kafka.ps1

# 2. 创建 Topics
.\deploy\create-kafka-topics.ps1

# 3. 运行数据生成器
py generator\generate_data.py --output kafka --kafka-bootstrap-servers localhost:9092 --rate 2000 --seed 20260927

# 4. 在 IDEA 运行 Flink 作业
```

### 日常开发
```powershell
# 启动
.\deploy\start-kafka.ps1

# 查看状态
.\deploy\status.ps1

# 停止
.\deploy\stop-kafka.ps1
```

### 完全清理数据
```powershell
docker compose --env-file deploy\.env -f deploy\docker-compose.yml down -v
```
⚠️ 注意：这会删除所有数据卷，包括 Kafka、MySQL、Redis 的所有数据。

## 服务地址

| 服务 | 地址 | 说明 |
|------|------|------|
| Kafka | localhost:9092 | IDEA 本地运行时使用 |
| ZooKeeper | localhost:2181 | ZooKeeper 客户端连接 |
| MySQL | localhost:3306 | 数据库连接 |
| Redis | localhost:6379 | Redis 客户端连接 |
| Flink Web UI | http://localhost:8081 | Docker Flink 集群 |
| Flink Web UI (本地) | http://localhost:8082 | IDEA 运行时的 Web UI |

## 故障排查

### Docker 启动失败
- 检查 Docker Desktop 是否正在运行
- 运行 `docker --version` 确认 Docker 已安装

### 端口被占用
- 检查端口是否被其他程序占用：
  ```powershell
  netstat -ano | findstr :9092
  netstat -ano | findstr :3306
  ```

### Topics 创建失败
- 确保 Kafka 已完全启动（等待 10-15 秒）
- 运行 `.\deploy\status.ps1` 查看 Kafka 是否 healthy

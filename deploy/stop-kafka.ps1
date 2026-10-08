# 停止 Kafka 和 ZooKeeper
Write-Host "正在停止 ZooKeeper 和 Kafka..." -ForegroundColor Yellow

$envFile = Join-Path $PSScriptRoot ".env"
$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"

docker compose --env-file $envFile -f $composeFile stop kafka zookeeper

Write-Host "`nKafka 和 ZooKeeper 已停止" -ForegroundColor Green
Write-Host "数据已保留，下次启动时数据仍然存在" -ForegroundColor Gray
Write-Host "如需完全删除数据，运行: docker compose --env-file deploy\.env -f deploy\docker-compose.yml down -v" -ForegroundColor Gray

# 启动所有服务（Kafka、MySQL、Redis、Flink）
Write-Host "正在启动所有服务..." -ForegroundColor Green

$envFile = Join-Path $PSScriptRoot ".env"
$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"

# 检查 .env 文件是否存在
if (-Not (Test-Path $envFile)) {
    Write-Host "未找到 .env 文件，正在复制 .env.example..." -ForegroundColor Yellow
    Copy-Item (Join-Path $PSScriptRoot ".env.example") $envFile
}

# 启动所有服务
docker compose --env-file $envFile -f $composeFile up -d

# 等待几秒
Start-Sleep -Seconds 5

# 显示服务状态
Write-Host "`n服务状态:" -ForegroundColor Green
docker compose --env-file $envFile -f $composeFile ps

Write-Host "`n所有服务已启动:" -ForegroundColor Cyan
Write-Host "  - Kafka:          localhost:9092" -ForegroundColor White
Write-Host "  - MySQL:          localhost:3306" -ForegroundColor White
Write-Host "  - Redis:          localhost:6379" -ForegroundColor White
Write-Host "  - Flink Web UI:   http://localhost:8081" -ForegroundColor White

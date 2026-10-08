# 停止所有服务
Write-Host "正在停止所有服务..." -ForegroundColor Yellow

$envFile = Join-Path $PSScriptRoot ".env"
$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"

docker compose --env-file $envFile -f $composeFile stop

Write-Host "`n所有服务已停止" -ForegroundColor Green
Write-Host "数据已保留，下次启动时数据仍然存在" -ForegroundColor Gray

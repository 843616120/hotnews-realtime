# 查看服务状态和日志
Write-Host "=== 服务状态 ===" -ForegroundColor Green

$envFile = Join-Path $PSScriptRoot ".env"
$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"

docker compose --env-file $envFile -f $composeFile ps

Write-Host "`n选择操作:" -ForegroundColor Cyan
Write-Host "1. 查看 Kafka 日志"
Write-Host "2. 查看 ZooKeeper 日志"
Write-Host "3. 查看所有日志"
Write-Host "4. 退出"

$choice = Read-Host "`n请输入选项 (1-4)"

switch ($choice) {
    "1" {
        Write-Host "`n正在查看 Kafka 日志 (Ctrl+C 退出)..." -ForegroundColor Yellow
        docker compose --env-file $envFile -f $composeFile logs -f kafka
    }
    "2" {
        Write-Host "`n正在查看 ZooKeeper 日志 (Ctrl+C 退出)..." -ForegroundColor Yellow
        docker compose --env-file $envFile -f $composeFile logs -f zookeeper
    }
    "3" {
        Write-Host "`n正在查看所有日志 (Ctrl+C 退出)..." -ForegroundColor Yellow
        docker compose --env-file $envFile -f $composeFile logs -f
    }
    "4" {
        Write-Host "退出" -ForegroundColor Gray
    }
    default {
        Write-Host "无效选项" -ForegroundColor Red
    }
}

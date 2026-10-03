$ErrorActionPreference = "Stop"

# 启动 Kafka 和 ZooKeeper
Write-Host "正在启动 ZooKeeper 和 Kafka..." -ForegroundColor Green

$envFile = Join-Path $PSScriptRoot ".env"
$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"

# 检查 .env 文件是否存在
if (-Not (Test-Path $envFile)) {
    Write-Host "未找到 .env 文件，正在复制 .env.example..." -ForegroundColor Yellow
    Copy-Item (Join-Path $PSScriptRoot ".env.example") $envFile
}

$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if ($null -ne $dockerCommand) {
    $docker = $dockerCommand.Source
} else {
    $docker = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin\docker.exe"
}

if (-not (Test-Path -LiteralPath $docker)) {
    throw "找不到 Docker CLI。请先安装或启动 Docker Desktop。"
}

# 启动服务
& $docker compose --env-file $envFile -f $composeFile up -d zookeeper kafka
if ($LASTEXITCODE -ne 0) {
    throw "ZooKeeper 和 Kafka 启动失败，请检查 Docker Desktop 和上面的错误信息。"
}

# 等待几秒
Start-Sleep -Seconds 3

# 显示服务状态
Write-Host "`n服务状态:" -ForegroundColor Green
& $docker compose --env-file $envFile -f $composeFile ps
if ($LASTEXITCODE -ne 0) {
    throw "启动命令已执行，但无法读取容器状态。"
}

Write-Host "`nKafka 已启动，连接地址: localhost:9092" -ForegroundColor Cyan
Write-Host "如需查看日志，运行: docker compose --env-file deploy\.env -f deploy\docker-compose.yml logs -f kafka" -ForegroundColor Gray

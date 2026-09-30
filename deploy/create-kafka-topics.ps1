$ErrorActionPreference = "Stop"

$composeFile = Join-Path $PSScriptRoot "docker-compose.yml"
$envFile = Join-Path $PSScriptRoot ".env"

if (-not (Test-Path -LiteralPath $envFile)) {
    $envFile = Join-Path $PSScriptRoot ".env.example"
}

$dockerCommand = Get-Command docker -ErrorAction SilentlyContinue
if ($null -ne $dockerCommand) {
    $docker = $dockerCommand.Source
} else {
    $docker = "C:\Users\Berry\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe"
}

if (-not (Test-Path -LiteralPath $docker)) {
    throw "Docker CLI was not found. Start Docker Desktop or add docker.exe to PATH."
}

& $docker compose --env-file $envFile -f $composeFile up -d zookeeper kafka

& $docker compose --env-file $envFile -f $composeFile exec -T kafka `
    kafka-topics --bootstrap-server kafka:29092 `
    --create --if-not-exists --topic topic_article `
    --partitions 3 --replication-factor 1

& $docker compose --env-file $envFile -f $composeFile exec -T kafka `
    kafka-topics --bootstrap-server kafka:29092 `
    --create --if-not-exists --topic topic_behavior `
    --partitions 6 --replication-factor 1

Write-Host ""
Write-Host "Kafka topics:"
& $docker compose --env-file $envFile -f $composeFile exec -T kafka `
    kafka-topics --bootstrap-server kafka:29092 --list

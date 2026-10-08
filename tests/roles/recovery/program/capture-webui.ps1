param(
    [string]$BaseUrl = 'http://localhost:8081',
    [string]$JobId = 'd933102253fcaaeb92dd34ce2faa8750',
    [string]$Phase = 'before-kill'
)
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../../../..')).Path
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$directory = Join-Path $root "tests/roles/recovery/evidence/$stamp-$Phase"
New-Item -ItemType Directory -Path $directory -Force | Out-Null

function Save-Endpoint([string]$Endpoint, [string]$Name) {
    try {
        $value = Invoke-RestMethod "$BaseUrl/$Endpoint" -TimeoutSec 10
        $value | ConvertTo-Json -Depth 60 | Set-Content (Join-Path $directory "$Name.json") -Encoding UTF8
        return $value
    } catch {
        $_.Exception.Message | Set-Content (Join-Path $directory "$Name-error.txt") -Encoding UTF8
        return $null
    }
}

# 每轮单独保存；包含开始/结束时刻，避免将顺序采样当成同时快照。
$started = (Get-Date).ToString('o')
$null = Save-Endpoint 'overview' 'overview'
$job = Save-Endpoint "jobs/$JobId" 'job'
$null = Save-Endpoint "jobs/$JobId/config" 'job-config'
$null = Save-Endpoint "jobs/$JobId/checkpoints/config" 'checkpoint-config'
$checkpoints = Save-Endpoint "jobs/$JobId/checkpoints" 'checkpoints'
$null = Save-Endpoint "jobs/$JobId/exceptions" 'exceptions'
if ($checkpoints.latest.completed) {
    $null = Save-Endpoint "jobs/$JobId/checkpoints/details/$($checkpoints.latest.completed.id)" 'completed-checkpoint-detail'
}
if ($checkpoints.latest.savepoint) {
    $null = Save-Endpoint "jobs/$JobId/checkpoints/details/$($checkpoints.latest.savepoint.id)" 'savepoint-detail'
}
foreach ($vertex in $job.vertices) {
    $null = Save-Endpoint "jobs/$JobId/vertices/$($vertex.id)" "vertex-$($vertex.id)"
    $null = Save-Endpoint "jobs/$JobId/vertices/$($vertex.id)/backpressure" "backpressure-$($vertex.id)"
    $catalog = Save-Endpoint "jobs/$JobId/vertices/$($vertex.id)/metrics" "metric-catalog-$($vertex.id)"
    $ids = @($catalog | Where-Object {
        $_.id -match 'Watermark|Records|records|Offset|offset|pending|p95|busyTimeMsPerSecond|backPressuredTimeMsPerSecond|idleTimeMsPerSecond|checkpointStartDelay|checkpointAlignment|join\.'
    } | ForEach-Object { $_.id })
    # 分批查询，避免 Source 分区指标令 URL 过长。
    for ($i = 0; $i -lt $ids.Count; $i += 35) {
        $last = [Math]::Min($i + 34, $ids.Count - 1)
        $query = [Uri]::EscapeDataString(($ids[$i..$last] -join ','))
        $null = Save-Endpoint "jobs/$JobId/vertices/$($vertex.id)/metrics?get=$query" "metrics-$($vertex.id)-$i"
    }
}
$tms = Save-Endpoint 'taskmanagers' 'taskmanagers'
foreach ($tm in $tms.taskmanagers) {
    $tmId = [Uri]::EscapeDataString($tm.id)
    $catalog = Save-Endpoint "taskmanagers/$tmId/metrics" 'tm-metric-catalog'
    $ids = @($catalog | Where-Object { $_.id -match 'Memory|CPU.Load|GarbageCollector' } | ForEach-Object { $_.id })
    $query = [Uri]::EscapeDataString(($ids -join ','))
    $null = Save-Endpoint "taskmanagers/$tmId/metrics?get=$query" 'tm-metrics'
}
[pscustomobject]@{
    phase = $Phase; job_id = $JobId; base_url = $BaseUrl
    started_at = $started; completed_at = (Get-Date).ToString('o')
} | ConvertTo-Json | Set-Content (Join-Path $directory 'capture.json') -Encoding UTF8
Write-Output $directory

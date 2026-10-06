param(
    [ValidateSet('article', 'ip')]
    [string]$Workload = 'article',
    [ValidateSet('hashmap', 'rocksdb')]
    [string]$Backend = 'rocksdb',
    [switch]$Optimized,
    [string]$OutputDir = 'tests/day4/evidence'
)

$ErrorActionPreference = 'Stop'
$compose = @('--env-file', 'deploy/.env', '-f', 'deploy/docker-compose.yml')
$baseUrl = 'http://localhost:8081'
$jar = '/opt/flink/usrlib/flink-rolesachieve-1.0-SNAPSHOT-all.jar'
$mode = if ($Optimized) { 'optimized' } else { 'baseline' }
$mainClass = if ($Workload -eq 'article') { 'ArticleHeatStateJob' } else { 'IpWindowStateJob' }
$groupPrefix = if ($Workload -eq 'article') { 'hotnews-day4-heat' } else { 'hotnews-day4-ip' }
$runId = "docker-" + (Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmssfff')
$groupPrefix = "$groupPrefix-$($mode)-verify-$runId"
$groups = @("$groupPrefix-article", "$groupPrefix-behavior")

function Invoke-ComposeExec {
    param([string[]]$CommandArgs)
    & docker compose @compose exec -T @CommandArgs
}

function Get-Bytes {
    param([string]$Value)
    if ($Value -match '([0-9.]+)\s*(GiB|MiB|KiB|GB|MB|kB|B)') {
        $number = [double]$Matches[1]
        $factor = switch ($Matches[2]) {
            'GiB' { 1GB } 'MiB' { 1MB } 'KiB' { 1KB }
            'GB' { 1e9 } 'MB' { 1e6 } 'kB' { 1e3 } 'B' { 1 }
        }
        return [long]($number * $factor)
    }
    return 0
}

function Get-RocksDbBytes {
    $files = Get-ChildItem 'D:\docker_data\flink\rocksdb' -Recurse -File -ErrorAction SilentlyContinue
    if ($null -eq $files) { return 0 }
    $sum = ($files | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $sum) { return 0 }
    return [long]$sum
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

foreach ($group in $groups) {
    try {
        Invoke-ComposeExec @('kafka', 'kafka-consumer-groups', '--bootstrap-server', 'kafka:29092', '--delete', '--group', $group) | Out-Null
    } catch {
        # A missing group is expected on the first run.
    }
}

$runArgs = @('--bounded', "--backend=$Backend", "--run-id=$runId")
if ($Optimized) { $runArgs += '--optimized' }
$start = Get-Date
$submitCommand = @('jobmanager', 'flink', 'run', '-d', '-c', $mainClass, $jar) + $runArgs
$submitOutput = @(Invoke-ComposeExec $submitCommand)
$submitText = $submitOutput -join "`n"
if ($submitText -notmatch 'JobID\s+([0-9a-f]{32})') {
    throw "无法从提交结果中解析 JobID：$submitText"
}
$jobId = $Matches[1]

$maxMemoryBytes = 0L
$maxRocksDbBytes = Get-RocksDbBytes
$states = New-Object System.Collections.Generic.List[string]
$job = $null
for ($i = 0; $i -lt 180; $i++) {
    $job = Invoke-RestMethod "$baseUrl/jobs/$jobId"
    $states.Add([string]$job.state)

    $taskManagerId = (& docker compose @compose ps -q taskmanager).Trim()
    if ($taskManagerId) {
        $memoryText = (& docker stats $taskManagerId --no-stream --format '{{.MemUsage}}') -join ' '
        $currentMemory = Get-Bytes $memoryText
        if ($currentMemory -gt $maxMemoryBytes) { $maxMemoryBytes = $currentMemory }
    }
    $currentRocksDb = Get-RocksDbBytes
    if ($currentRocksDb -gt $maxRocksDbBytes) { $maxRocksDbBytes = $currentRocksDb }

    if ($job.state -in @('FINISHED', 'FAILED', 'CANCELED', 'SUSPENDED')) { break }
    Start-Sleep -Seconds 2
}

if ($null -eq $job -or $job.state -notin @('FINISHED', 'FAILED', 'CANCELED', 'SUSPENDED')) {
    throw "作业未在 6 分钟内结束：$jobId"
}

$checkpoints = Invoke-RestMethod "$baseUrl/jobs/$jobId/checkpoints"
$vertices = foreach ($vertex in $job.vertices) {
    $subtasks = foreach ($subtask in 0..($vertex.parallelism - 1)) {
        try {
            $metrics = Invoke-RestMethod "$baseUrl/jobs/$jobId/vertices/$($vertex.id)/subtasks/$subtask/metrics?get=numRecordsIn,numRecordsOut,busyTimeMsPerSecond,backPressuredTimeMsPerSecond"
        } catch {
            $metrics = @()
        }
        [ordered]@{ subtask = $subtask; metrics = $metrics }
    }
    [ordered]@{
        id = $vertex.id
        name = $vertex.name
        parallelism = $vertex.parallelism
        status = $vertex.status
        metrics = $vertex.metrics
        subtasks = $subtasks
    }
}

$behaviorVertex = $vertices | Where-Object { $_.name -like '*Behavior_Source*' } | Select-Object -First 1
$inputRecords = 0L
if ($behaviorVertex -and $behaviorVertex.metrics.'write-records') {
    $inputRecords = [long]$behaviorVertex.metrics.'write-records'
}
$durationSeconds = [double]$job.duration / 1000.0
$throughput = if ($durationSeconds -gt 0) { $inputRecords / $durationSeconds } else { 0 }
$hadRestart = $states -contains 'RESTARTING'
$validFullBatch = ($inputRecords -eq 100000) -and (-not $hadRestart) -and ($job.state -eq 'FINISHED')
$end = Get-Date
$stamp = $start.ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
$prefix = "docker-$Workload-$mode-$Backend-$stamp"
$logFile = Join-Path $OutputDir "$prefix-taskmanager.log"
(& docker compose @compose logs --no-color --since $start.ToUniversalTime().ToString('o') taskmanager) | Set-Content -Encoding UTF8 $logFile

$evidence = [ordered]@{
    workload = $Workload
    mode = $mode
    backend = $Backend
    job_id = $jobId
    job_name = $job.name
    state = $job.state
    started_at = $start.ToUniversalTime().ToString('o')
    collected_at = $end.ToUniversalTime().ToString('o')
    duration_ms = $job.duration
    input_behavior_records = $inputRecords
    expected_behavior_records = 100000
    had_restart = $hadRestart
    valid_full_batch = $validFullBatch
    throughput_events_per_second = $throughput
    p95_source_to_keyed_ms = $null
    p95_note = 'Production job has no Source-to-keyed Probe; checkpoint and busy time are not p95.'
    max_taskmanager_memory_bytes = $maxMemoryBytes
    rocksdb_peak_bytes = $maxRocksDbBytes
    rocksdb_final_bytes = Get-RocksDbBytes
    states = $states
    checkpoints = $checkpoints
    vertices = $vertices
    taskmanager_log = $logFile
}
$evidenceFile = Join-Path $OutputDir "$prefix.json"
$evidence | ConvertTo-Json -Depth 30 | Set-Content -Encoding UTF8 $evidenceFile
Write-Output ($evidence | ConvertTo-Json -Depth 4)
Write-Output "Evidence: $evidenceFile"

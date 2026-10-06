param(
    [Parameter(Mandatory = $true)][ValidatePattern('^[a-z0-9-]{1,40}$')][string]$Stage,
    [ValidateRange(1, 10000)][int]$Rate = 2000,
    [ValidateRange(5, 120)][int]$Seconds = 30,
    [ValidateRange(0, 1000)][int]$DelayMs = 0,
    [ValidateRange(1, 4)][int]$SinkParallelism = 2,
    [ValidateRange(1, 10000)][int]$Keys = 200,
    [ValidateSet('mysql', 'redis')][string]$Target = 'mysql',
    [ValidateRange(5, 120)][int]$SampleSeconds = 25
)

# 第六天隔离运行器：检查共享集群空闲，提交当前 JAR，立即采指标并等待作业结束。
# 思路：每次试验都保存唯一原始样本；不停止旧作业、不修改生产 Topic/表或 Docker 数据卷。
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$jar = Join-Path $root 'flink-job/flink-rolesachieve/target/flink-rolesachieve-1.0-SNAPSHOT-all.jar'
if (-not (Test-Path -LiteralPath $jar)) { throw "请先 Maven 打包：$jar" }
$jobs = Invoke-RestMethod 'http://localhost:8081/jobs/overview' -TimeoutSec 10
$running = @($jobs.jobs | Where-Object state -eq 'RUNNING')
if ($running.Count -gt 0) { throw "共享集群已有 RUNNING 作业，先确认容量；不自动停止：$($running.jid -join ', ')" }

docker cp $jar deploy-jobmanager-1:/tmp/day6-benchmark.jar
if ($LASTEXITCODE -ne 0) { throw 'Docker 拷贝 JAR 失败' }
$submitted = docker exec deploy-jobmanager-1 flink run -d -c Day6BackpressureJob `
    /tmp/day6-benchmark.jar --rate $Rate --seconds $Seconds --delay-ms $DelayMs `
    --sink-parallelism $SinkParallelism --keys $Keys --target $Target 2>&1
if ($LASTEXITCODE -ne 0) { throw "Flink 提交失败: $submitted" }
$text = $submitted -join "`n"
if ($text -notmatch 'JobID ([a-f0-9]{32})') { throw "没有获取到作业 ID: $text" }
$jobId = $Matches[1]
Write-Host "Day 6 $Stage JobID: $jobId"
try {
    node (Join-Path $PSScriptRoot 'capture.mjs') $jobId $Stage $SampleSeconds
    if ($LASTEXITCODE -ne 0) { throw 'REST 指标采集失败' }
} finally {
    $deadline = (Get-Date).AddSeconds([Math]::Max(180, $Seconds + 150))
    do {
        Start-Sleep -Seconds 2
        $status = (Invoke-RestMethod "http://localhost:8081/jobs/$jobId" -TimeoutSec 10).state
    } while ($status -eq 'RUNNING' -and (Get-Date) -lt $deadline)
    Write-Host "Day 6 $Stage terminal status: $status"
}
if ($status -ne 'FINISHED') { throw "未正常结束：$jobId ($status)；请检查 Flink REST 异常和检查点" }

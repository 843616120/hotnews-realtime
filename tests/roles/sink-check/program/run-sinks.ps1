param(
  [Parameter(Mandatory = $true)]
  [string]$RuleRunId,
  [string]$RunId = ("ab-sink-" + [guid]::NewGuid().ToString("N").Substring(0, 8))
)
$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "../../../..")).Path
$rules = Join-Path $PSScriptRoot "../../boundary-test/evidence/$RuleRunId"
$run = Join-Path $PSScriptRoot "../evidence/$RunId"
if (Test-Path -LiteralPath $run) { throw "证据目录已存在，不覆盖：$run" }
New-Item -ItemType Directory -Path (Join-Path $run "classes") | Out-Null
# 只复制本窗口的规则结果，不读取业务表构造测试预期。
Copy-Item -LiteralPath (Join-Path $rules "role-a-output.jsonl") -Destination $run
Copy-Item -LiteralPath (Join-Path $rules "role-b-output.jsonl") -Destination $run
$jar = Join-Path $root "flink-job/flink-rules/target/flink-rules-1.0-SNAPSHOT-all.jar"
$slf4j = Join-Path $env:USERPROFILE ".m2/repository/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
$dependencies = "$(Join-Path $rules 'classes');$jar;$slf4j"
& "$env:JAVA_HOME/bin/javac.exe" -encoding UTF-8 -cp $dependencies -d (Join-Path $run "classes") `
    (Join-Path $PSScriptRoot "LiveEvidence.java") *> (Join-Path $run "compile.log")
if ($LASTEXITCODE -ne 0) { throw "Sink 测试编译失败" }
& "$env:JAVA_HOME/bin/java.exe" "-Dfile.encoding=UTF-8" -cp "$(Join-Path $run 'classes');$dependencies" `
    LiveEvidence sink-check $run $RunId *> (Join-Path $run "sink-runtime.log")
if ($LASTEXITCODE -ne 0) { throw "Sink 测试失败，见运行日志" }
& node (Join-Path $PSScriptRoot "verify-sinks.mjs") $rules $run
if ($LASTEXITCODE -ne 0) { throw "临时表 SQL 对照失败" }
Write-Output "Sink 证据目录：$run"

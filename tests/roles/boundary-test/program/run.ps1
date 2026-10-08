param([string]$RunId = ("20261007-" + [guid]::NewGuid().ToString("N").Substring(0, 8)))
$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "../../../..")).Path
$run = Join-Path $PSScriptRoot "../evidence/$RunId"
if (Test-Path -LiteralPath $run) { throw "证据目录已存在，不覆盖：$run" }
New-Item -ItemType Directory -Path (Join-Path $run "classes") | Out-Null
$jar = Join-Path $root "flink-job/flink-rules/target/flink-rules-1.0-SNAPSHOT-all.jar"
$slf4j = Join-Path $env:USERPROFILE ".m2/repository/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
$cp = "$jar;$slf4j"
$sources = @(
  "flink-job/flink-common/src/main/java/constant/Constant.java",
  "flink-job/flink-common/src/main/java/model/CategoryRankingSnapshot.java",
  "flink-job/flink-common/src/main/java/util/FlinkMetricsUtil.java",
  "flink-job/flink-common/src/main/java/util/FlinkRuntimeUtil.java",
  "flink-job/flink-common/src/main/java/util/FlinkSinkUtil.java",
  "flink-job/flink-common/src/main/java/util/FlinkSourceUtil.java",
  "flink-job/flink-common/src/main/java/util/RoleStreamUtil.java",
  "flink-job/flink-join/src/main/java/com/agd/flink/join/ArticleJoinBehavior.java",
  "flink-job/flink-rules/src/main/java/com/agd/flink/roles/rolea/Achieve_roleA.java",
  "flink-job/flink-rules/src/main/java/com/agd/flink/roles/roleb/Achieve_roleB.java"
) | ForEach-Object { Join-Path $root $_ }
# 编译当前 A/B 及其依赖到独立目录，不写共享 target、不编译 Rule C。
$before = $sources | Get-FileHash -Algorithm SHA256
$before | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 (Join-Path $run "source-before.json")
& "$env:JAVA_HOME/bin/javac.exe" -encoding UTF-8 -cp $cp -d (Join-Path $run "classes") @sources `
    (Join-Path $PSScriptRoot "RulesAbAcceptance.java") `
    *> (Join-Path $run "compile.log")
if ($LASTEXITCODE -ne 0) { throw "编译失败，见 $run/compile.log" }
$runtime = "$(Join-Path $run 'classes');$cp"
foreach ($role in @("a", "b")) {
  & "$env:JAVA_HOME/bin/java.exe" "-Dfile.encoding=UTF-8" -cp $runtime RulesAbAcceptance $role `
      (Join-Path $PSScriptRoot "../inputs/20261007-ab/role-$role") `
      (Join-Path $run "role-$role-output.jsonl") *> (Join-Path $run "role-$role-runtime.log")
  if ($LASTEXITCODE -ne 0) { throw "Rule $role 运行失败，见运行日志" }
}
$sources | Get-FileHash -Algorithm SHA256 | ConvertTo-Json -Depth 5 |
    Set-Content -Encoding utf8 (Join-Path $run "source-after.json")
& node --no-warnings (Join-Path $PSScriptRoot "verify.mjs") $run
if ($LASTEXITCODE -ne 0) { throw "SQL 对照有差异，见 $run/verification.json" }
Write-Output "证据目录：$run"

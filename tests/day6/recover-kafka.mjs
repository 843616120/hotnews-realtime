import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { writeFile } from "node:fs/promises";
import path from "node:path";

/**
 * 第六天 Kafka 积压追平验收。
 * 思路：同一消费组从已提交位点继续读取，提升慢 Sink 并行度，
 * 连续两次看到 Lag 为零且检查点成功才判定追平；仅取消本次提交的作业。
 */
const [topic, group, parallelText = "4"] = process.argv.slice(2);
const parallelism = Number(parallelText);
if (!/^hotnews-day6-[a-z0-9-]+$/.test(topic || "")
    || !/^hotnews-day6-[a-z0-9-]+$/.test(group || "")
    || !Number.isInteger(parallelism) || parallelism < 1 || parallelism > 4) {
  throw new Error("用法: node tests/day6/recover-kafka.mjs <隔离Topic> <隔离消费组> [Sink并行度 1..4]");
}
const exec = promisify(execFile);
const docker = (container, ...command) => exec("docker", ["exec", container, ...command],
  { timeout: 120000, maxBuffer: 100000 });
const rest = async (url) => {
  const response = await fetch(`http://localhost:8081${url}`);
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  return response.json();
};
const jobs = await rest("/jobs/overview");
if (jobs.jobs.some(job => job.state === "RUNNING")) throw new Error("集群已有 RUNNING 作业");
const jar = path.resolve("flink-job/flink-rolesachieve/target/flink-rolesachieve-1.0-SNAPSHOT-all.jar");
await exec("docker", ["cp", jar, "deploy-jobmanager-1:/tmp/day6-benchmark.jar"]);
const submitted = await docker("deploy-jobmanager-1", "flink", "run", "-d", "-c",
  "Day6BackpressureJob", "/tmp/day6-benchmark.jar", "--kafka-topic", topic,
  "--kafka-group", group, "--delay-ms", "2", "--sink-parallelism", String(parallelism));
const jobId = submitted.stdout.match(/JobID ([a-f0-9]{32})/)?.[1];
if (!jobId) throw new Error(`提交失败: ${submitted.stdout}`);
console.log(`追平作业: ${jobId}`);
const capture = spawn(process.execPath, [path.join(import.meta.dirname, "capture.mjs"),
  jobId, "kafka-recovery", "45"], {
  stdio: "inherit", env: { ...process.env, DAY6_KAFKA_GROUP: group },
});
const start = Date.now();
let zeroChecks = 0;
let lastLag = null;
const lagSamples = [];
try {
  for (let i = 0; i < 45; i++) {
    await new Promise(resolve => setTimeout(resolve, 2000));
    const job = await rest(`/jobs/${jobId}`);
    if (job.state !== "RUNNING") throw new Error(`追平作业状态: ${job.state}`);
    const text = (await docker("deploy-kafka-1", "kafka-consumer-groups",
      "--bootstrap-server", "kafka:29092", "--describe", "--group", group)).stdout;
    const rows = text.split(/\r?\n/).filter(line => line.trim().startsWith(group + " "));
    if (rows.length === 0) continue;
    lastLag = rows.reduce((sum, line) => sum + Number(line.trim().split(/\s+/)[5]), 0);
    const checkpoints = await rest(`/jobs/${jobId}/checkpoints`);
    lagSamples.push({ at: new Date().toISOString(), lag: lastLag,
      completedCheckpoints: checkpoints.counts.completed, raw: text });
    console.log(`lag=${lastLag} completedCheckpoints=${checkpoints.counts.completed}`);
    zeroChecks = lastLag === 0 && checkpoints.counts.completed > 0 ? zeroChecks + 1 : 0;
    if (zeroChecks >= 2) break;
  }
  const result = { topic, group, jobId, parallelism, lastLag,
    lagSamples, caughtUp: zeroChecks >= 2, elapsedSeconds: (Date.now() - start) / 1000,
    collectedAt: new Date().toISOString() };
  const file = path.resolve("tests/day6/results", `kafka-recovery-${Date.now()}.json`);
  await writeFile(file, JSON.stringify(result, null, 2), { flag: "wx" });
  console.log(file);
  if (!result.caughtUp) throw new Error(`90 秒内没有确认追平；最后 Lag: ${lastLag}`);
} finally {
  if (capture.exitCode === null) capture.kill();
  const canceled = await docker("deploy-jobmanager-1", "flink", "cancel", jobId);
  console.log(canceled.stdout.trim());
}

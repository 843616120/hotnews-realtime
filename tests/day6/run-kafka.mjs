import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";

/**
 * 第六天隔离 Kafka 压测编排器。
 * 思路：创建唯一短保留 Topic，提交专用作业，Kafka 性能工具限速发压；
 * 并行采集真实 Lag，完成后仅取消本次 Job ID，保留证据供容量推算。
 */
const exec = promisify(execFile);
const [stage, rateText, secondsText, delayText, parallelText = "2"] = process.argv.slice(2);
const rate = Number(rateText), seconds = Number(secondsText);
const delay = Number(delayText), parallelism = Number(parallelText);
if (!/^[a-z0-9-]{1,25}$/.test(stage || "") || !Number.isInteger(rate) || rate < 1 || rate > 10000
    || !Number.isInteger(seconds) || seconds < 5 || seconds > 60
    || !Number.isInteger(delay) || delay < 0 || delay > 1000
    || !Number.isInteger(parallelism) || parallelism < 1 || parallelism > 4) {
  throw new Error("用法: node tests/day6/run-kafka.mjs <stage> <rate 1..10000> <seconds 5..60> <delay-ms 0..1000> [sink-parallelism 1..4]");
}
const rest = "http://localhost:8081";
const read = async (url) => {
  const response = await fetch(rest + url);
  if (!response.ok) throw new Error(`Flink REST ${url}: ${response.status}`);
  return response.json();
};
const jobs = await read("/jobs/overview");
if (jobs.jobs.some(job => job.state === "RUNNING")) {
  throw new Error("共享 Flink 集群已有 RUNNING 作业；不自动干预");
}
const suffix = Date.now().toString(36);
const topic = `hotnews-day6-${stage}-${suffix}`;
const group = `hotnews-day6-${stage}-${suffix}`;
const docker = (container, ...command) => exec("docker", ["exec", container, ...command],
  { timeout: 120000, maxBuffer: 300000 });
await docker("deploy-kafka-1", "kafka-topics", "--bootstrap-server", "kafka:29092",
  "--create", "--topic", topic, "--partitions", "3", "--replication-factor", "1",
  "--config", "retention.ms=3600000");
const jar = path.resolve("flink-job/flink-rolesachieve/target/flink-rolesachieve-1.0-SNAPSHOT-all.jar");
await exec("docker", ["cp", jar, "deploy-jobmanager-1:/tmp/day6-benchmark.jar"],
  { timeout: 60000 });
const submitted = await docker("deploy-jobmanager-1", "flink", "run", "-d", "-c",
  "Day6BackpressureJob", "/tmp/day6-benchmark.jar", "--kafka-topic", topic,
  "--kafka-group", group, "--delay-ms", String(delay), "--sink-parallelism", String(parallelism));
const match = submitted.stdout.match(/JobID ([a-f0-9]{32})/);
if (!match) throw new Error(`无法解析作业 ID: ${submitted.stdout}`);
const jobId = match[1];
console.log(JSON.stringify({ stage, jobId, topic, group, rate, seconds, delay, parallelism }));
let capture;
let producerOutput = "";
try {
  for (let i = 0; i < 20; i++) {
    const job = await read(`/jobs/${jobId}`);
    if (job.state !== "RUNNING") throw new Error(`Flink 作业提前退出: ${job.state}`);
    if (job.vertices?.length && job.vertices.every(v => v.status === "RUNNING")) break;
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  capture = spawn(process.execPath, [path.join(import.meta.dirname || path.dirname(new URL(import.meta.url).pathname),
    "capture.mjs"), jobId, `kafka-${stage}`, String(Math.min(60, seconds + 18))], {
    stdio: "inherit", env: { ...process.env, DAY6_KAFKA_GROUP: group },
  });
  const perf = await docker("deploy-kafka-1", "kafka-producer-perf-test",
    "--topic", topic, "--num-records", String(rate * seconds),
    "--record-size", "200", "--throughput", String(rate),
    "--producer-props", "bootstrap.servers=kafka:29092", "acks=1");
  producerOutput = perf.stdout;
  console.log(producerOutput);
  await new Promise((resolve, reject) => {
    capture.on("error", reject);
    capture.on("exit", code => code === 0 ? resolve() : reject(new Error(`采集退出码 ${code}`)));
  });
  const finalLag = await docker("deploy-kafka-1", "kafka-consumer-groups",
    "--bootstrap-server", "kafka:29092", "--describe", "--group", group);
  const folder = path.resolve("tests/day6/results");
  await mkdir(folder, { recursive: true });
  const result = path.join(folder, `kafka-${stage}-${suffix}-run.json`);
  await writeFile(result, JSON.stringify({ stage, jobId, topic, group, rate, seconds, delay,
    parallelism, producerOutput, finalLag: finalLag.stdout, collectedAt: new Date().toISOString(),
  }, null, 2), { flag: "wx" });
  console.log(result);
} finally {
  if (capture && capture.exitCode === null) capture.kill();
  const canceled = await docker("deploy-jobmanager-1", "flink", "cancel", jobId);
  console.log(`仅取消本次作业 ${jobId}: ${canceled.stdout.trim()}`);
}

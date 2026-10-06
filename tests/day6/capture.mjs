import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

/**
 * 第六天只读指标采集器：定时读取 Flink REST 的算子、检查点和 TaskManager 指标。
 * 思路：保存未经修饰的采样和采样间隔，让吞吐/积压结论能从原始计数复核。
 */
const [jobId, label, secondsText = "25"] = process.argv.slice(2);
const seconds = Number(secondsText);
if (!/^[0-9a-f]{32}$/.test(jobId || "") || !/^[a-z0-9-]{1,40}$/.test(label || "")
    || !Number.isInteger(seconds) || seconds < 5 || seconds > 180) {
  throw new Error("用法: node tests/day6/capture.mjs <32位JobID> <短标签> [5..180秒]");
}
const base = process.env.FLINK_REST_URL || "http://localhost:8081";
const kafkaGroup = process.env.DAY6_KAFKA_GROUP;
if (kafkaGroup && !/^hotnews-day6-[a-z0-9-]+$/.test(kafkaGroup)) {
  throw new Error("消费组必须是 hotnews-day6-*");
}
const exec = promisify(execFile);
const metricSuffixes = [
  "numRecordsIn", "numRecordsOut", "numRecordsInPerSecond", "numRecordsOutPerSecond",
  "busyTimeMsPerSecond", "backPressuredTimeMsPerSecond", "idleTimeMsPerSecond",
  "day6_emitted", "day6_consumed", "day6_last_latency_ms",
];
const request = async (uri) => {
  const response = await fetch(base + uri);
  if (!response.ok) throw new Error(`${uri}: HTTP ${response.status}`);
  return response.json();
};
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const metricValues = (items) => Object.fromEntries(items.map(item => [item.id, item.value]));
const samples = [];
const metricIds = new Map();
let taskmanagers;
try {
  taskmanagers = await request("/taskmanagers");
  const deadline = Date.now() + seconds * 1000;
  while (Date.now() <= deadline) {
    const job = await request(`/jobs/${jobId}`);
    const vertices = await Promise.all(job.vertices.map(async vertex => {
      const details = await request(`/jobs/${jobId}/vertices/${vertex.id}`);
      const subtasks = await Promise.all((details.subtasks || []).map(async subtask => {
        const endpoint = `/jobs/${jobId}/vertices/${vertex.id}/subtasks/${subtask.subtask}/metrics`;
        try {
          let names = metricIds.get(endpoint);
          if (!names?.length) {
            const available = await request(endpoint);
            names = available.map(item => item.id).filter(id =>
              metricSuffixes.some(suffix => id === suffix || id.endsWith(`.${suffix}`)));
            metricIds.set(endpoint, names);
          }
          return { index: subtask.subtask, state: subtask.status,
            metrics: metricValues(await request(`${endpoint}?get=${names.join(",")}`)) };
        } catch (error) {
          return { index: subtask.subtask, state: subtask.status, error: String(error) };
        }
      }));
      return { id: vertex.id, name: vertex.name, status: vertex.status, subtasks };
    }));
    const checkpoints = await request(`/jobs/${jobId}/checkpoints`);
    let kafkaLag = null;
    if (kafkaGroup) {
      try {
        const { stdout } = await exec("docker", ["exec", "deploy-kafka-1", "kafka-consumer-groups",
          "--bootstrap-server", "kafka:29092", "--describe", "--group", kafkaGroup], {
          timeout: 8000, maxBuffer: 100_000,
        });
        kafkaLag = { raw: stdout, rows: stdout.split(/\r?\n/).filter(line =>
          line.includes("hotnews-day6-")).map(line => {
          const columns = line.trim().split(/\s+/);
          return { topic: columns[1], partition: Number(columns[2]),
            committed: Number(columns[3]), logEnd: Number(columns[4]), lag: Number(columns[5]) };
        }).filter(row => Number.isFinite(row.lag)) };
      } catch (error) {
        kafkaLag = { error: error.message };
      }
    }
    const tm = await Promise.all((taskmanagers.taskmanagers || []).map(async manager => {
      const info = await request(`/taskmanagers/${manager.id}`);
      const ids = await request(`/taskmanagers/${manager.id}/metrics`);
      const names = ids.map(value => value.id).filter(name =>
        /GarbageCollector.*(Count|Time)$|Status\.JVM\.(Memory|CPU)|Status\.Flink\.Memory/.test(name));
      const metrics = names.length ? await request(`/taskmanagers/${manager.id}/metrics?get=${names.join(",")}`) : [];
      return { id: manager.id, info, metrics: metricValues(metrics) };
    }));
    samples.push({ at: new Date().toISOString(), jobState: job.state, vertices,
      checkpoints, kafkaLag, taskmanagers: tm });
    if (job.state !== "RUNNING") break;
    await wait(2000);
  }
  const folder = path.resolve("tests/day6/results");
  await mkdir(folder, { recursive: true });
  const file = path.join(folder, `${label}-${new Date().toISOString().replaceAll(":", "-")}.json`);
  await writeFile(file, JSON.stringify({
    jobId, label, intervalSeconds: 2, restUrl: base, collectedAt: new Date().toISOString(),
    definitions: {
      queuedProxy: "day6_emitted - day6_consumed; 非 Kafka Lag，且多个并行子任务须求和",
      latency: "day6_last_latency_ms 是单个 Sink 子任务最后一条记录，不是 p95",
      throughput: "对两个样本的计数差除以实际采样秒数，非目标发速",
    }, samples,
  }, null, 2), { flag: "wx" });
  console.log(file);
} catch (error) {
  console.error(`采集失败；已采 ${samples.length} 个样本: ${error.message}`);
  process.exitCode = 1;
}

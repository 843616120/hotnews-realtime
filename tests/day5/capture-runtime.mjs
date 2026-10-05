import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [stage, jobId] = process.argv.slice(2);
if (!/^[a-z0-9-]+$/.test(stage || "") || !/^[0-9a-f]{32}$/.test(jobId || "")) {
  throw new Error("Usage: node tests/day5/capture-runtime.mjs <stage> <32-char-job-id>");
}
async function flink(path) {
  const result = await fetch(`http://localhost:8081${path}`);
  if (!result.ok) throw new Error(`${path}: HTTP ${result.status}`);
  return result.json();
}
const redis = (...cmd) => execFileSync(process.execPath, [
  join(import.meta.dirname, "docker-engine.mjs"), "exec", "redis", "redis-cli", "--raw", ...cmd,
], { encoding: "utf8" }).trim().replaceAll("\r", "");
const [job, checkpoints, config] = await Promise.all([
  flink(`/jobs/${jobId}`), flink(`/jobs/${jobId}/checkpoints`), flink(`/jobs/${jobId}/config`),
]);
const [window, revision, rankingText] = redis("HMGET",
  "hotnews:top5:latest", "window_start_ms", "revision", "ranking").split("\n");
const ranking = rankingText && rankingText !== "(nil)" ? JSON.parse(rankingText) : [];
const snapshot = {
  stage, capturedAt: new Date().toISOString(), jobId, state: job.state,
  vertices: job.vertices.map(v => ({ name: v.name, status: v.status, parallelism: v.parallelism })),
  parallelism: config["execution-config"]["job-parallelism"],
  checkpoints: {
    counts: checkpoints.counts,
    latestCompleted: checkpoints.latest.completed && {
      id: checkpoints.latest.completed.id,
      durationMs: checkpoints.latest.completed.end_to_end_duration,
      path: checkpoints.latest.completed.external_path,
    },
    restored: checkpoints.latest.restored,
  },
  redis: {
    windowStartMs: window, revision, rankingLength: ranking.length,
    ttlSeconds: Number(redis("TTL", "hotnews:top5:latest")),
    semanticSha256: ranking.length ? createHash("sha256").update(JSON.stringify(
      ranking.map(({ detect_time, ...business }) => business))).digest("hex") : null,
  },
  mysql: "not queried: host credentials require explicit user configuration",
};
const dir = join(import.meta.dirname, "evidence");
mkdirSync(dir, { recursive: true });
const target = join(dir, `${stage}.json`);
writeFileSync(target, JSON.stringify(snapshot, null, 2) + "\n", { flag: "wx" });
console.log(JSON.stringify({
  file: target, state: snapshot.state, runningVertices: snapshot.vertices.filter(v => v.status === "RUNNING").length,
  checkpoints: snapshot.checkpoints, redis: snapshot.redis, mysql: snapshot.mysql,
}, null, 2));

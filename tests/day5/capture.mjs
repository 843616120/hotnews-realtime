import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [stage, jobId] = process.argv.slice(2);
if (!/^[a-z0-9-]+$/.test(stage || "") || !/^[0-9a-f]{32}$/.test(jobId || "")) {
  throw new Error("Usage: node tests/day5/capture.mjs <stage> <32-char-job-id>");
}
if (!process.env.MYSQL_PASSWORD) {
  throw new Error("Set MYSQL_PASSWORD from user-provided credentials before capturing MySQL data");
}

function docker(service, ...cmd) {
  return execFileSync(process.execPath,
    [join(import.meta.dirname, "docker-engine.mjs"), "exec", service, ...cmd],
    { encoding: "utf8", maxBuffer: 4 * 1024 * 1024 }).trim().replaceAll("\r", "");
}

function sql(query) {
  return docker("mysql", "mysql",
    `--user=${process.env.MYSQL_USER || "root"}`,
    `--database=${process.env.MYSQL_DATABASE || "hotnews"}`,
    "--batch", "--raw", "--skip-column-names", "-e", query);
}

async function flink(path) {
  const response = await fetch(`http://localhost:8081${path}`);
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

const [job, checkpoints] = await Promise.all([
  flink(`/jobs/${jobId}`), flink(`/jobs/${jobId}/checkpoints`),
]);
const counts = sql("SELECT (SELECT COUNT(*) FROM clean_behavior),(SELECT COUNT(*) FROM article_alert),(SELECT COUNT(*) FROM category_rank),(SELECT COUNT(*) FROM ip_alert WHERE retracted=0),(SELECT COUNT(*) FROM pipeline_event);");
const ranks = sql("SELECT window_start_ms,rank_no,category,score,revision FROM category_rank ORDER BY window_start_ms,rank_no;");
const alerts = sql("SELECT window_start_ms,article_id,click_count FROM article_alert ORDER BY window_start_ms,article_id;");
const exceptions = sql("SELECT event_type,COUNT(*) FROM pipeline_event GROUP BY event_type ORDER BY event_type;");
const redis = docker("redis", "redis-cli", "--raw", "HMGET",
  "hotnews:top5:latest", "window_start_ms", "revision", "ranking").split("\n");
const ttl = docker("redis", "redis-cli", "--raw", "TTL", "hotnews:top5:latest");
const ranking = redis[2] && redis[2] !== "(nil)" ? JSON.parse(redis[2]) : [];
const semanticRanking = ranking.map(({ detect_time, ...business }) => business);
const snapshot = {
  stage, capturedAt: new Date().toISOString(), jobId, state: job.state,
  checkpoints: {
    counts: checkpoints.counts,
    latestCompleted: checkpoints.latest.completed && {
      id: checkpoints.latest.completed.id,
      durationMs: checkpoints.latest.completed.end_to_end_duration,
      path: checkpoints.latest.completed.external_path,
      checkpointedBytes: checkpoints.latest.completed.checkpointed_size,
    },
    restored: checkpoints.latest.restored,
    latestFailed: checkpoints.latest.failed,
  },
  mysql: {
    columns: ["clean", "article_alert", "category_rank", "active_ip_alert", "pipeline_event"],
    counts: counts.split("\t").map(Number),
    alerts: alerts.split("\n").filter(Boolean).map(line => line.split("\t")),
    ranks: ranks.split("\n").filter(Boolean).map(line => line.split("\t")),
    exceptionCounts: exceptions.split("\n").filter(Boolean).map(line => line.split("\t")),
  },
  redis: {
    windowStartMs: redis[0], revision: redis[1], ttlSeconds: Number(ttl),
    rankingLength: ranking.length,
    rankingSha256: redis[2] && redis[2] !== "(nil)"
      ? createHash("sha256").update(redis[2]).digest("hex") : null,
    rankingSemanticSha256: ranking.length
      ? createHash("sha256").update(JSON.stringify(semanticRanking)).digest("hex") : null,
  },
};
const dir = join(import.meta.dirname, "evidence");
mkdirSync(dir, { recursive: true });
const target = join(dir, `${stage}.json`);
writeFileSync(target, JSON.stringify(snapshot, null, 2) + "\n", { flag: "wx" });
console.log(JSON.stringify({
  file: target, state: snapshot.state, checkpoints: snapshot.checkpoints,
  mysqlCounts: snapshot.mysql.counts, redis: snapshot.redis,
}, null, 2));

import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const final = path.resolve(process.argv[2]);
const analysis = JSON.parse(fs.readFileSync(path.join(final, 'live-analysis.json'), 'utf8'));
const log = fs.readFileSync(analysis.logs.b.path, 'utf8');
const db = new DatabaseSync(path.join(final, 'kafka-baseline.sqlite'), { readOnly: true });
const joined = new Map(db.prepare('SELECT * FROM joined').all().map(row => [row.event_id, row]));
const articleRaw = new Map(db.prepare('SELECT line FROM article_raw').all().map(({ line }) => {
  const row = JSON.parse(line); return [row.article_id, row];
}));
const rawById = new Map(db.prepare(
  'SELECT r.line FROM joined j JOIN behavior_raw r ON r.rowid=j.sequence'
).all().map(({ line }) => {
  const row = JSON.parse(line); return [row.event_id, row];
}));
const late = new Map(), invalid = [], categories = {}, articles = {};
for (const [index, line] of log.split(/\r?\n/).entries()) {
  const match = /\bROLE_B_LATE(?::\d+)?>\s*(\{.*\})/.exec(line);
  if (!match) continue;
  const value = JSON.parse(match[1]), raw = rawById.get(value.event_id);
  const candidate = joined.get(value.event_id);
  const fields = ['article_id', 'event_time', 'action', 'user_id', 'ip', 'read_duration_ms', 'ingest_time'];
  if (!raw || !candidate || fields.some(field => raw[field] !== value[field])
      || articleRaw.get(raw.article_id)?.category !== value.category) {
    invalid.push({ line: index + 1, event_id: value.event_id, reason: '旁路与 Kafka 原始有效输入不一致' });
    continue;
  }
  late.set(value.event_id, { line: index + 1, sequence: candidate.sequence,
    event_id: value.event_id, window_start_ms: Math.floor(candidate.event_ms / 600000) * 600000,
    category: candidate.category, article_id: candidate.article_id });
}
for (const row of late.values()) {
  const categoryKey = `${row.window_start_ms}|${row.category}`;
  const articleKey = `${row.window_start_ms}|${row.article_id}`;
  categories[categoryKey] = (categories[categoryKey] || 0) + 1;
  articles[articleKey] = (articles[articleKey] || 0) + 1;
}
db.close();
const revisions = analysis.diagnostic.kafka.b.differences.filter(row => row.field === 'revision')
  .map(row => ({ key: row.key, offline: row.expected, online: row.actual, gap: row.expected - row.actual,
    rawLateInWindow: [...late.values()].filter(value => value.window_start_ms === Number(row.key.split('|')[0])).length }));
const result = { purpose: '原始输入与旁路的贡献对账，不用旁路生成独立 SQL 预期，也不声称实际 Watermark 已验证',
  uniqueLateEvents: late.size, invalid, categories, articles, revisions, events: [...late.values()] };
fs.writeFileSync(path.join(final, 'late-contribution-audit.json'), JSON.stringify(result, null, 2), { flag: 'wx' });
console.log(JSON.stringify({ uniqueLateEvents: late.size, invalid: invalid.length, categories, revisions }, null, 2));
if (invalid.length || revisions.some(row => row.gap !== row.rawLateInWindow)) process.exitCode = 1;

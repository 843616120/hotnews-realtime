import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

const capture = path.resolve(process.argv[2]);
const final = path.resolve(process.argv[3]);
const base = import.meta.dirname;
const read = file => JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''));
const lines = file => fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(line => line.trim());
const hash = text => createHash('sha256').update(text).digest('hex');
const save = (name, value) => fs.writeFileSync(path.join(final, name), JSON.stringify(value, null, 2), { flag: 'wx' });
const sourceDirs = {
  a: path.resolve(base, '../../../../generator/generator/data'),
  b: path.resolve(base, '../../../../generator/generator/role2_data'),
};
const snapshot = read(path.join(capture, 'live-snapshot.json'));
const baseline = read(path.join(process.argv[4] ? path.resolve(process.argv[4]) : final, 'production-baseline.json'));
const comparisons = { a: {}, b: {} }, sql = {}, manifests = {};
const input = path.join(final, 'kafka-input');
fs.mkdirSync(input, { recursive: true });
for (const [topic, filename] of [['topic_article', 'article_stream.jsonl'], ['topic_behavior', 'behavior_stream.jsonl']]) {
  const records = lines(path.join(capture, `${topic}.jsonl`)).map(JSON.parse);
  const actual = records.map(row => row.value);
  fs.writeFileSync(path.join(input, filename), actual.join('\n') + '\n', { flag: 'wx' });
  for (const role of ['a', 'b']) {
    const original = lines(path.join(sourceDirs[role], filename));
    const left = new Map(), right = new Map(), fileIds = new Map();
    for (const row of original) {
      left.set(hash(row), (left.get(hash(row)) || 0) + 1);
      const value = JSON.parse(row), variants = fileIds.get(value.event_id) || new Map();
      variants.set(hash(row), value);
      fileIds.set(value.event_id, variants);
    }
    for (const row of actual) right.set(hash(row), (right.get(hash(row)) || 0) + 1);
    let missing = 0, extra = 0;
    for (const [id, count] of left) missing += Math.max(0, count - (right.get(id) || 0));
    for (const [id, count] of right) extra += Math.max(0, count - (left.get(id) || 0));
    const reused = actual.filter(row => {
      const value = JSON.parse(row);
      return fileIds.has(value.event_id) && !fileIds.get(value.event_id).has(hash(row));
    }).map(row => {
      const value = JSON.parse(row);
      return { event_id: value.event_id, kafka: value, jsonl: [...fileIds.get(value.event_id).values()][0] };
    });
    comparisons[role][topic] = { inputFile: path.join(sourceDirs[role], filename),
      jsonlLines: original.length, kafkaRecords: actual.length,
      rawMissingFromKafka: missing, rawExtraInKafka: extra, identicalRawMultiset: missing === 0 && extra === 0,
      reusedIdDifferentPayloadCount: reused.length, reusedIdExamples: reused.slice(0, 5) };
  }
  manifests[topic] = { captureFile: path.join(capture, `${topic}.jsonl`),
    sha256: hash(fs.readFileSync(path.join(capture, `${topic}.jsonl`))), ranges: snapshot.kafka?.[topic]?.ranges };
}

function baselineFor(directory, name) {
  const target = path.join(final, `${name}.sqlite`);
  if (fs.existsSync(target)) throw new Error(`不覆盖数据库：${target}`);
  const db = new DatabaseSync(target);
  db.exec('CREATE TABLE article_raw(line TEXT); CREATE TABLE behavior_raw(line TEXT);'
    + 'CREATE TABLE schedule(sequence INTEGER PRIMARY KEY,wm_before_ms INTEGER); BEGIN;');
  for (const [table, file] of [['article_raw', 'article_stream.jsonl'], ['behavior_raw', 'behavior_stream.jsonl']]) {
    const insert = db.prepare(`INSERT INTO ${table} VALUES(?)`);
    for (const line of lines(path.join(directory, file))) insert.run(line);
  }
  db.exec('COMMIT');
  db.exec(fs.readFileSync(path.join(base, '../../boundary-test/sql/prepare.sql'), 'utf8'));
  const result = { counts: {}, database: target };
  for (const table of ['article_raw', 'article_clean', 'behavior_raw', 'behavior_clean', 'joined']) {
    result.counts[table] = db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n;
  }
  result.range = db.prepare('SELECT MIN(event_ms) AS min_ms,MAX(event_ms) AS max_ms FROM joined').get();
  result.actionCounts = db.prepare('SELECT action,COUNT(*) AS n FROM joined GROUP BY action').all();
  result.categoryCounts = db.prepare('SELECT category,COUNT(*) AS n FROM joined GROUP BY category').all();
  for (const role of ['a', 'b']) {
    result[role] = db.prepare(fs.readFileSync(path.join(base, `../../boundary-test/sql/role_${role}.sql`), 'utf8')).all({ online: 0 });
    if (role === 'b') result[role].forEach(row => { row.top_articles = JSON.parse(row.top_articles); });
  }
  db.close();
  return result;
}
sql.kafka = baselineFor(input, 'kafka-baseline');
sql.aInput = baselineFor(sourceDirs.a, 'rule-a-baseline');
sql.bInput = baselineFor(sourceDirs.b, 'rule-b-baseline');
const semantic = articles => articles.map(({ article_id, title, score }) => ({ article_id, title, score }));
function compare(role, expected, actual) {
  const id = row => `${row.window_start_ms}|${role === 'a' ? row.article_id : row.rank}`;
  const fields = role === 'a' ? ['window_end_ms', 'article_id', 'title', 'category', 'click_count']
    : ['window_end_ms', 'rank', 'category', 'score', 'revision', 'top_articles'];
  const left = new Map(expected.map(row => [id(row), row]));
  const right = new Map(actual.map(row => [id(row), row]));
  const differences = [];
  for (const [key, row] of left) {
    if (!right.has(key)) differences.push({ key, type: 'missing', expected: row });
    else for (const field of fields) {
      const a = field === 'top_articles' ? semantic(row[field]) : row[field];
      const b = field === 'top_articles' ? semantic(right.get(key)[field]) : right.get(key)[field];
      if (JSON.stringify(a) !== JSON.stringify(b)) differences.push({ key, field, expected: a, actual: b });
    }
  }
  for (const [key, row] of right) if (!left.has(key)) differences.push({ key, type: 'extra', actual: row });
  return { expectedRows: expected.length, actualRows: actual.length, differences };
}
const mysqlA = read(path.join(capture, 'article_alert.json'));
const mysqlB = read(path.join(capture, 'category_rank.json')).map(row => ({
  ...row, rank: row.rank_no, top_articles: JSON.parse(row.top_articles),
}));
const diagnostic = {
  fixed: { a: compare('a', sql.aInput.a, mysqlA), b: compare('b', sql.bInput.b, mysqlB) },
  kafka: { a: compare('a', sql.kafka.a, mysqlA), b: compare('b', sql.kafka.b, mysqlB) },
};
const logs = {};
for (const role of ['a', 'b']) {
  const file = path.resolve(base, `../../sql-check/logs/role-${role}.log`);
  const content = lines(file);
  const pattern = new RegExp(`\\bROLE_${role.toUpperCase()}(?::\\d+)?>\\s*(\\{.*\\})`);
  const results = new Map(), times = [], late = [];
  const malformed = [];
  const latePattern = new RegExp(`\\bROLE_${role.toUpperCase()}_LATE(?::\\d+)?>\\s*(\\{.*\\})`);
  for (const [index, line] of content.entries()) {
    const match = pattern.exec(line);
    if (match) {
      try {
        const item = JSON.parse(match[1]);
        times.push(item.detect_time);
        const key = `${Date.parse(item.window_start)}|${role === 'a' ? item.article_id : item.rank}`;
        const previous = results.get(key);
        if (!previous || (role === 'a' ? item.click_count >= previous.click_count : item.revision >= previous.revision)) {
          results.set(key, item);
        }
      } catch (error) { malformed.push({ line: index + 1, message: error.message }); }
    }
    const lateMatch = latePattern.exec(line);
    if (lateMatch) late.push({ line: index + 1, value: JSON.parse(lateMatch[1]) });
  }
  const normalized = [...results.values()].map(item => ({
    ...item, window_start_ms: Date.parse(item.window_start), window_end_ms: Date.parse(item.window_end),
  }));
  logs[role] = { path: file, sha256: hash(fs.readFileSync(file)), rawResultLines: times.length,
    finalBusinessRows: results.size, detectTimeMin: times.sort()[0], detectTimeMax: times.at(-1),
    malformed, lateRecords: late.length, lateExamples: late.slice(0, 5),
    errors: content.flatMap((line, index) => /deadlock|Deadlock|Caused by|ERROR /.test(line)
      ? [{ line: index + 1, text: line }] : []).slice(0, 30),
    vsJsonl: compare(role, role === 'a' ? sql.aInput.a : sql.bInput.b, normalized),
    vsKafka: compare(role, sql.kafka[role], normalized),
    provenance: '诊断对照：缺少运行起始位点与窗口 Watermark，不作为同批通过/失败判据' };
}
const clean = read(path.join(capture, 'clean_behavior.json'));
const cleanIds = new Set(clean.map(row => row.event_id));
const capturedKafkaRawIds = new Set(lines(path.join(input, 'behavior_stream.jsonl')).map(line => JSON.parse(line).event_id));
// 用独立 SQL 保留的合法副本对照；不能让同 ID 后到的脏副本覆盖合法原文。
const kafkaDb = new DatabaseSync(path.join(final, 'kafka-baseline.sqlite'), { readOnly: true });
const capturedKafkaIds = new Map(kafkaDb.prepare(
  'SELECT j.event_id,r.line FROM joined j JOIN behavior_raw r ON r.rowid=j.sequence'
).all().map(({ line }) => {
  const row = JSON.parse(line); return [row.event_id, row];
}));
kafkaDb.close();
const cleanConflicts = clean.filter(row => capturedKafkaIds.has(row.event_id)).filter(row => {
  const raw = capturedKafkaIds.get(row.event_id);
  return ['article_id', 'action', 'event_time', 'user_id', 'ip', 'read_duration_ms']
    .some(field => row[field] !== raw[field]);
});
const audit = read(path.join(capture, 'pipeline_event.json'));
const auditCounts = {};
for (const row of audit) auditCounts[row.event_type] = (auditCounts[row.event_type] || 0) + 1;
const redisState = snapshot.redis;
const ranking = redisState?.value?.ranking ? JSON.parse(redisState.value.ranking) : [];
const redisWindow = Number(redisState?.value?.window_start_ms);
const matchingMysql = mysqlB.filter(row => row.window_start_ms === redisWindow).map(row => ({
  ...row, window_start: new Date(row.window_start_ms).toISOString(),
  window_end: new Date(row.window_end_ms).toISOString(),
}));
const redisMysql = compare('b', matchingMysql, ranking.map(row => ({
  ...row, window_start_ms: Date.parse(row.window_start), window_end_ms: Date.parse(row.window_end),
})));
const result = {
  capturedAt: snapshot.captured_at, sourceDirs, manifests, comparisons, sql,
  consumerGroups: snapshot.consumer_groups, redis: { ...redisState, ranking },
  redisVsMysql: redisMysql, diagnostic,
  mysqlClean: { count: clean.length,
    recordsOutsideCurrentKafkaRawIds: clean.filter(row => !capturedKafkaRawIds.has(row.event_id)).length,
    recordsOutsideCurrentKafkaValidIds: clean.filter(row => !capturedKafkaIds.has(row.event_id)).length,
    missingCurrentKafkaValidIds: [...capturedKafkaIds.keys()].filter(id => !cleanIds.has(id)).length,
    currentKafkaIdPayloadConflicts: cleanConflicts.length, examples: cleanConflicts.slice(0, 5) },
  pipelineEventCounts: auditCounts, logs,
  watermark: '未验证：快照时 8081/8083/8084/8085 均无运行作业可读取窗口 Watermark',
  baselineStable: {
    a: JSON.stringify(baseline.inputs?.a?.counts || baseline.counts) === JSON.stringify(sql.aInput.counts),
    b: baseline.inputs?.b ? JSON.stringify(baseline.inputs.b.counts) === JSON.stringify(sql.bInput.counts)
      : '旧证据将 A 的目录用于 B，已由本次独立基准更正',
  },
};
save('live-analysis.json', result);
console.log(JSON.stringify({
  comparisons, kafkaClean: sql.kafka.counts, kafkaBaseline: [sql.kafka.a.length, sql.kafka.b.length],
  mysqlClean: result.mysqlClean, auditCounts,
  redisVsMysqlDifferences: redisMysql.differences.length,
  diagnostic: Object.fromEntries(Object.entries(diagnostic).map(([name, roles]) =>
    [name, Object.fromEntries(Object.entries(roles).map(([role, comparison]) => [role, {
      sql: comparison.expectedRows, mysql: comparison.actualRows, differences: comparison.differences.length,
      example: comparison.differences[0],
    }]))])),
  logs: Object.fromEntries(Object.entries(logs).map(([role, info]) => [role, {
    lines: info.rawResultLines, final: info.finalBusinessRows, late: info.lateRecords,
    vsJsonlDifferences: info.vsJsonl.differences.length, vsKafkaDifferences: info.vsKafka.differences.length,
    first: info.detectTimeMin, last: info.detectTimeMax, errors: info.errors.length,
  }])),
}, null, 2));

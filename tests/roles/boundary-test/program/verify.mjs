import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

const directory = path.resolve(process.argv[2]);
const base = path.resolve(import.meta.dirname, '..');
const read = file => JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''));
const lines = file => fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(line => line.trim());
const sha = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const checks = [];
function check(name, expected, actual) {
  checks.push({ name, expected, actual, passed: JSON.stringify(expected) === JSON.stringify(actual) });
}
function load(input, schedule) {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE article_raw(line TEXT); CREATE TABLE behavior_raw(line TEXT);'
    + 'CREATE TABLE schedule(sequence INTEGER PRIMARY KEY, wm_before_ms INTEGER);');
  const manifest = {};
  db.exec('BEGIN');
  for (const [table, file] of [['article_raw', 'article_stream.jsonl'], ['behavior_raw', 'behavior_stream.jsonl']]) {
    const filename = path.join(input, file), raw = lines(filename);
    const insert = db.prepare(`INSERT INTO ${table} VALUES(?)`);
    for (const line of raw) insert.run(line);
    manifest[file] = { path: filename, sha256: sha(filename), lines: raw.length };
  }
  if (schedule) {
    const insert = db.prepare('INSERT INTO schedule VALUES(?,?)');
    for (const step of schedule.filter(step => step.kind === 'event')) insert.run(step.sequence, step.wm_before_ms);
  }
  db.exec('COMMIT');
  db.exec(fs.readFileSync(path.join(base, 'sql/prepare.sql'), 'utf8'));
  return { db, manifest };
}
function expected(db, role, online) {
  return db.prepare(fs.readFileSync(path.join(base, `sql/role_${role}.sql`), 'utf8')).all({ online: online ? 1 : 0 })
    .map(row => ({ ...row, ...(role === 'b' ? { top_articles: JSON.parse(row.top_articles) } : {}) }));
}
function normalized(row, role, actual) {
  return {
    window_start_ms: actual ? Date.parse(row.window_start) : row.window_start_ms,
    window_end_ms: actual ? Date.parse(row.window_end) : row.window_end_ms,
    ...(role === 'a'
      ? { article_id: row.article_id, title: row.title, category: row.category, click_count: row.click_count }
      : { rank: row.rank, category: row.category, score: row.score,
        revision: row.revision, top_articles: row.top_articles.map(article => ({
          article_id: article.article_id, title: article.title, score: article.score,
        })) }),
  };
}
function compare(sql, actual, role) {
  const key = row => `${row.window_start_ms}|${role === 'a' ? row.article_id : row.rank}`;
  const left = new Map(sql.map(row => [key(row), row])), right = new Map(actual.map(row => [key(row), row]));
  const differences = [];
  for (const [id, row] of left) {
    if (!right.has(id)) differences.push({ key: id, type: 'missing', expected: row });
    else for (const field of Object.keys(row)) {
      if (JSON.stringify(row[field]) !== JSON.stringify(right.get(id)[field])) {
        differences.push({ key: id, field, expected: row[field], actual: right.get(id)[field] });
      }
    }
  }
  for (const [id, row] of right) if (!left.has(id)) differences.push({ key: id, type: 'extra', actual: row });
  return differences;
}
const evidence = {};
for (const role of ['a', 'b']) {
  const input = path.join(base, `inputs/20261007-ab/role-${role}`);
  const schedule = read(path.join(input, 'schedule.json'));
  const { db, manifest } = load(input, schedule);
  const all = expected(db, role, false).map(row => normalized(row, role, false));
  const online = expected(db, role, true).map(row => normalized(row, role, false));
  const outputs = lines(path.join(directory, `role-${role}-output.jsonl`)).map(JSON.parse);
  const actual = new Map();
  const intermediate = outputs.filter(row => row.kind === 'result').map(row => row.value);
  for (const row of intermediate) {
    const id = `${Date.parse(row.window_start)}|${role === 'a' ? row.article_id : row.rank}`;
    const previous = actual.get(id);
    if (!previous || (role === 'a' ? row.click_count >= previous.click_count : row.revision >= previous.revision)) {
      actual.set(id, row);
    }
  }
  const final = [...actual.values()].map(row => normalized(row, role, true));
  const late = outputs.filter(row => row.kind === 'late').map(row => row.value);
  const diffs = compare(online, final, role);
  check(`Rule ${role.toUpperCase()} SQL online differences`, 0, diffs.length);
  const start = Date.parse('2026-09-27T00:00:00Z');
  if (role === 'a') {
    for (const id of ['count-999', 'count-1000']) check(`A ${id} never alerts`, 0,
      intermediate.filter(row => row.article_id === id).length);
    check('A 1001 clicks in five overlapping windows', 5,
      final.filter(row => row.article_id === 'count-1001' && row.click_count === 1001).length);
    for (const [id, count] of [['boundary-before', 5], ['boundary-at', 4], ['boundary-after', 4]]) {
      check(`A ${id} alert windows`, count, final.filter(row => row.article_id === id).length);
    }
    check('A boundary itself excluded from [00:00,00:05)', false,
      final.some(row => row.article_id === 'boundary-at' && row.window_start_ms === start));
    check('A allowed late crosses threshold in all five windows', 5,
      intermediate.filter(row => row.article_id === 'late-probe' && row.click_count === 1001).length);
    check('A cleanup before 1ms retains all five windows', 5,
      intermediate.filter(row => row.article_id === 'late-probe' && row.click_count === 1002).length);
    check('A cleanup boundary retains only four windows', 4,
      final.filter(row => row.article_id === 'late-probe' && row.click_count === 1004).length);
    check('A first cleaned window stays at 1002', 1002,
      final.find(row => row.article_id === 'late-probe' && row.window_start_ms === start)?.click_count);
    check('A fully expired event side output', 1, late.length);
  } else {
    const firstRevision = Math.min(...intermediate.filter(row => Date.parse(row.window_start) === start).map(row => row.revision));
    const initial = intermediate.filter(row => Date.parse(row.window_start) === start && row.revision === firstRevision)
      .sort((a, b) => a.rank - b.rank);
    check('B initial three actions count once, duplicate ignored', 8, initial[0]?.score);
    check('B category Top5 ties alphabetical', ['alpha', 'bravo', 'charlie', 'delta', 'epsilon'],
      initial.map(row => row.category));
    check('B article Top5 ties alphabetical and truncation', ['alpha-1', 'alpha-2', 'alpha-3', 'alpha-4', 'alpha-5'],
      initial[0]?.top_articles.map(row => row.article_id));
    check('B each category has at most five articles', true,
      intermediate.every(row => Array.isArray(row.top_articles) && row.top_articles.length <= 5));
    const next = final.filter(row => row.window_start_ms === start + 600000);
    check('B boundary and boundary+1ms enter next window, fewer than five categories', [1, 'next', 2],
      [next.length, next[0]?.category, next[0]?.score]);
    check('B allowed late reverses ranking', 'bravo',
      final.find(row => row.window_start_ms === start && row.rank === 1)?.category);
    check('B cleanup before 1ms included, boundary and after excluded', [10, 26],
      [final.find(row => row.window_start_ms === start && row.rank === 1)?.score,
        final.find(row => row.window_start_ms === start && row.rank === 1)?.revision]);
    check('B fully expired side output', 2, late.length);
    const snapshots = outputs.filter(row => row.kind === 'redis_snapshot').map(row => row.value);
    check('B Redis snapshot key and TTL', true,
      snapshots.length > 0 && snapshots.every(row => row.key === 'hotnews:top5:latest' && row.ttl_seconds === 7200));
    check('B serialized ranking complete (no unresolved references)', true,
      snapshots.every(row => row.ranking.length > 0 && row.ranking.length <= 5
        && row.ranking.every(item => Array.isArray(item.top_articles))));
  }
  evidence[role] = { manifest, schedule, sqlAll: all, sqlOnline: online, actual: final,
    differences: diffs, offlineVsOnline: compare(all, final, role), late, printedResults: intermediate.length };
  db.close();
}
const sourceBefore = read(path.join(directory, 'source-before.json'));
const sourceAfter = read(path.join(directory, 'source-after.json'));
check('Production source unchanged during compile/run',
  sourceBefore.map(row => [row.Path, row.Hash]), sourceAfter.map(row => [row.Path, row.Hash]));

// A/B 固定输入不同，不能共用一个生成器目录构造线上基准。
const production = { inputs: {} };
for (const [role, folder] of [['a', 'data'], ['b', 'role2_data']]) {
  const { db, manifest } = load(path.resolve(base, `../../../generator/generator/${folder}`));
  const counts = {};
  for (const table of ['article_raw', 'article_clean', 'behavior_raw', 'behavior_clean', 'joined']) {
    counts[table] = db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n;
  }
  production.inputs[role] = {
    manifest, counts, eventRange: db.prepare('SELECT MIN(event_ms) AS min_ms, MAX(event_ms) AS max_ms FROM joined').get(),
    actions: db.prepare('SELECT action,COUNT(*) AS n FROM joined GROUP BY action ORDER BY action').all(),
  };
  production[role] = expected(db, role, false);
  db.close();
}
fs.writeFileSync(path.join(directory, 'production-baseline.json'), JSON.stringify(production, null, 2), { flag: 'wx' });
fs.writeFileSync(path.join(directory, 'verification.json'), JSON.stringify({ checks, evidence }, null, 2), { flag: 'wx' });
for (const item of checks) console.log(`${item.passed ? 'PASS' : 'FAIL'} ${item.name}`);
for (const role of ['a', 'b']) {
  console.log(`Rule ${role.toUpperCase()} 固定输入 SQL: ${JSON.stringify(production.inputs[role].counts)} 结果=${production[role].length}`);
}
if (checks.some(item => !item.passed)) process.exitCode = 1;

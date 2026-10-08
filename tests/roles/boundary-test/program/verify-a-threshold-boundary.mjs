import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

// 复用真实算子输出，只对本次要求的阈值和事件时间边界建立独立 SQL 预期。
const base = path.resolve(import.meta.dirname, '..');
const run = path.resolve(process.argv[2]);
const input = path.join(base, 'inputs/20261007-ab/role-a');
const read = file => JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''));
const lines = file => fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(line => line.trim());
const sha = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const ids = ['count-999', 'count-1000', 'count-1001', 'boundary-before', 'boundary-at', 'boundary-after'];
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE article_raw(line TEXT); CREATE TABLE behavior_raw(line TEXT); BEGIN;');
const manifest = {};
for (const [table, filename] of [['article_raw', 'article_stream.jsonl'], ['behavior_raw', 'behavior_stream.jsonl']]) {
  const file = path.join(input, filename);
  const raw = lines(file);
  const insert = db.prepare(`INSERT INTO ${table} VALUES(?)`);
  for (const line of raw) insert.run(line);
  manifest[filename] = { path: file, lines: raw.length, sha256: sha(file) };
}
db.exec('COMMIT;');
db.exec(fs.readFileSync(path.join(base, 'sql/prepare.sql'), 'utf8'));
const sql = fs.readFileSync(path.join(base, 'sql/role_a_threshold_boundary.sql'), 'utf8');
const windowCounts = db.prepare(sql).all();
const expected = db.prepare(`SELECT * FROM (${sql}) WHERE click_count > 1000`).all();
const output = lines(path.join(run, 'role-a-output.jsonl')).map(JSON.parse);
const results = output.filter(row => row.kind === 'result' && ids.includes(row.value.article_id));
const actual = results.map(({ value }) => ({
  window_start_ms: Date.parse(value.window_start), window_end_ms: Date.parse(value.window_end),
  article_id: value.article_id, title: value.title, category: value.category, click_count: value.click_count,
}));
const key = row => `${row.window_start_ms}|${row.article_id}`;
const right = new Map(actual.map(row => [key(row), row]));
const differences = [];
for (const row of expected) {
  const found = right.get(key(row));
  if (!found) differences.push({ key: key(row), type: 'missing', expected: row });
  else for (const field of Object.keys(row)) {
    if (row[field] !== found[field]) differences.push({ key: key(row), field, expected: row[field], actual: found[field] });
  }
}
for (const row of actual) {
  if (!expected.some(item => key(item) === key(row))) differences.push({ key: key(row), type: 'extra', actual: row });
}
const checks = [];
const check = (name, expected, actual) => checks.push({
  name, expected, actual, passed: JSON.stringify(expected) === JSON.stringify(actual),
});
for (const [id, clicks, alerts] of [['count-999', 999, 0], ['count-1000', 1000, 0], ['count-1001', 1001, 5]]) {
  check(`${id} 独立 SQL 五窗口点击数`, Array(5).fill(clicks),
    windowCounts.filter(row => row.article_id === id).map(row => row.click_count));
  check(`${id} 实际告警窗口数`, alerts, right.size && actual.filter(row => row.article_id === id).length);
}
const start = Date.parse('2026-09-27T00:00:00Z');
for (const [id, count, alerts] of [['boundary-before', 1001, 5], ['boundary-at', 1000, 4], ['boundary-after', 1000, 4]]) {
  check(`${id} 在 [00:00,00:05) 的 SQL 点击数`, count,
    windowCounts.find(row => row.article_id === id && row.window_start_ms === start)?.click_count);
  check(`${id} 实际告警窗口数`, alerts, actual.filter(row => row.article_id === id).length);
  check(`${id} 在 [00:00,00:05) 是否告警`, count > 1000,
    actual.some(row => row.article_id === id && row.window_start_ms === start));
}
check('1001 次点击的实际五个窗口起点', [0, 1, 2, 3, 4].map(n => start + n * 60000),
  actual.filter(row => row.article_id === 'count-1001').map(row => row.window_start_ms).sort((a, b) => a - b));
check('share/comment 样本确实存在', 4, db.prepare(
  "SELECT COUNT(*) AS n FROM joined WHERE article_id IN ('count-999','count-1000') AND action IN ('share','comment')",
).get().n);
check('1000 点击样本包含一条重复 click', 1001, db.prepare(
  "SELECT COUNT(*) AS n FROM behavior_raw WHERE json_extract(line,'$.article_id')='count-1000' AND json_extract(line,'$.action')='click'",
).get().n);
check('重复 click 在 SQL 中去重后仍为 1000', 1000, db.prepare(
  "SELECT COUNT(*) AS n FROM joined WHERE article_id='count-1000' AND action='click'",
).get().n);
check('六个样本的独立 SQL 正例行数', 18, expected.length);
check('六个样本的实际唯一告警键数', 18, right.size);
check('逐窗口、文章、字段 SQL 差异', 0, differences.length);
const before = read(path.join(run, 'source-before.json'));
const after = read(path.join(run, 'source-after.json'));
check('编译和运行期间生产源码未变', before.map(row => [row.Path, row.Hash]), after.map(row => [row.Path, row.Hash]));
const schedule = read(path.join(input, 'schedule.json'));
const behaviors = lines(path.join(input, 'behavior_stream.jsonl')).map(JSON.parse);
check('六个样本均在首次 Watermark 前到达', true,
  schedule.filter(step => step.kind === 'event' && ids.includes(behaviors[step.sequence - 1].article_id))
    .every(step => step.wm_before_ms === null));
const result = {
  verified_at: new Date().toISOString(), scope: 'Rule A 阈值和事件时间边界，不包含迟到清理验收',
  manifest, checks, sqlWindowCounts: windowCounts, sqlAlerts: expected, actual, differences,
  sourceBefore: before, sourceAfter: after,
};
fs.writeFileSync(path.join(run, 'threshold-boundary-verification.json'), JSON.stringify(result, null, 2), { flag: 'wx' });
db.close();
for (const item of checks) console.log(`${item.passed ? 'PASS' : 'FAIL'} ${item.name}`);
console.log(`SQL=${expected.length} Flink=${right.size} differences=${differences.length}`);
if (checks.some(item => !item.passed)) process.exitCode = 1;

import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const args = process.argv.slice(2);
function option(name, fallback = null) {
  const index = args.indexOf(name);
  if (index < 0) return fallback;
  if (!args[index + 1] || args[index + 1].startsWith('--')) {
    throw new Error(`${name} 缺少参数`);
  }
  return args[index + 1];
}

const dataDir = path.resolve(option('--data-dir', path.join(import.meta.dirname, '../../../../generator/generator/total_data')));
const snapshotDir = path.resolve(option('--snapshot'));
const output = path.resolve(option('--output', path.join(snapshotDir, 'mysql-vs-sql.json')));
const readJson = file => JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''));
const lines = file => fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(line => line.trim());

function loadBaseline() {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE article_raw(line TEXT NOT NULL); CREATE TABLE behavior_raw(line TEXT NOT NULL); BEGIN');
  for (const [table, name] of [['article_raw', 'article_stream.jsonl'], ['behavior_raw', 'behavior_stream.jsonl']]) {
    const insert = db.prepare(`INSERT INTO ${table}(line) VALUES (?)`);
    for (const line of lines(path.join(dataDir, name))) insert.run(line);
  }
  db.exec('COMMIT');
  db.exec(fs.readFileSync(path.join(import.meta.dirname, '../sql/prepare.sql'), 'utf8'));
  const result = {};
  for (const name of ['a', 'b', 'c']) {
    result[name] = db.prepare(fs.readFileSync(path.join(import.meta.dirname, `../sql/role_${name}.sql`), 'utf8')).all();
  }
  db.close();
  return result;
}

function normalizeArticles(value) {
  const rows = typeof value === 'string' ? JSON.parse(value) : value;
  return rows.map(row => ({ article_id: row.article_id, title: row.title, score: Number(row.score) }));
}

function normalizeIds(value) {
  const rows = typeof value === 'string' ? JSON.parse(value) : value;
  return rows.map(String);
}

function key(role, row, actual) {
  if (role === 'a') return `${row.window_start_ms}|${row.article_id}`;
  if (role === 'b') return `${row.window_start_ms}|${actual ? row.rank_no : row.rank}`;
  const minute = actual ? row.alert_minute_ms : Math.floor(row.window_end_ms / 60000) * 60000;
  return `${minute}|${row.ip}`;
}

function comparable(role, row, actual) {
  if (role === 'a') return {
    window_end_ms: Number(row.window_end_ms), title: row.title, category: row.category,
    click_count: Number(row.click_count),
  };
  if (role === 'b') return {
    window_end_ms: Number(row.window_end_ms), category: row.category, score: Number(row.score),
    top_articles: normalizeArticles(row.top_articles),
  };
  return {
    window_start_ms: Number(row.window_start_ms), window_end_ms: Number(row.window_end_ms),
    article_count: Number(row.article_count), click_count: Number(row.click_count),
    avg_read_duration_ms: Number(row.avg_read_duration_ms), article_ids: normalizeIds(row.article_ids),
    retracted: actual ? Boolean(row.retracted) : false,
  };
}

function compare(role, expectedRows, actualRows) {
  const expected = new Map(expectedRows.map(row => [key(role, row, false), row]));
  const actual = new Map(actualRows.map(row => [key(role, row, true), row]));
  const differences = [];
  for (const [id, row] of expected) {
    if (!actual.has(id)) {
      differences.push({ key: id, type: 'missing', expected: comparable(role, row, false) });
      continue;
    }
    const left = comparable(role, row, false);
    const right = comparable(role, actual.get(id), true);
    for (const field of Object.keys(left)) {
      const equal = field === 'avg_read_duration_ms'
        ? Math.abs(left[field] - right[field]) < 0.001
        : JSON.stringify(left[field]) === JSON.stringify(right[field]);
      if (!equal) differences.push({ key: id, field, expected: left[field], actual: right[field] });
    }
  }
  for (const [id, row] of actual) {
    if (!expected.has(id)) differences.push({ key: id, type: 'extra', actual: comparable(role, row, true) });
  }
  return {
    sqlRows: expected.size,
    mysqlRows: actual.size,
    missing: differences.filter(row => row.type === 'missing').length,
    extra: differences.filter(row => row.type === 'extra').length,
    fieldDifferences: differences.filter(row => !row.type).length,
    passed: differences.length === 0,
    differences,
  };
}

const baseline = loadBaseline();
const actual = {
  a: readJson(path.join(snapshotDir, 'article_alert.json')),
  b: readJson(path.join(snapshotDir, 'category_rank.json')),
  c: readJson(path.join(snapshotDir, 'ip_alert.json')),
};
const report = {
  generated_at: new Date().toISOString(),
  data_dir: dataDir,
  snapshot_dir: snapshotDir,
  input_counts: {
    article_raw: lines(path.join(dataDir, 'article_stream.jsonl')).length,
    behavior_raw: lines(path.join(dataDir, 'behavior_stream.jsonl')).length,
  },
  roles: {
    a: compare('a', baseline.a, actual.a),
    b: compare('b', baseline.b, actual.b),
    c: compare('c', baseline.c, actual.c),
  },
};
fs.writeFileSync(output, JSON.stringify(report, null, 2), { flag: 'w' });
for (const [role, result] of Object.entries(report.roles)) {
  console.log(`ROLE_${role.toUpperCase()} SQL=${result.sqlRows} MySQL=${result.mysqlRows}`
    + ` 缺失=${result.missing} 多出=${result.extra} 字段差异=${result.fieldDifferences}`
    + ` ${result.passed ? 'PASS' : 'FAIL'}`);
}
if (Object.values(report.roles).some(result => !result.passed)) process.exitCode = 1;

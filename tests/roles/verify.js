// 从固定生成器 JSONL 构造 SQLite 基准；Flink 日志只用于对照，不参与基准计算。
const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');

const args = process.argv.slice(2);
function option(name) {
  const index = args.indexOf(name);
  if (index < 0) return null;
  if (!args[index + 1] || args[index + 1].startsWith('--')) {
    throw new Error(`${name} 缺少参数`);
  }
  return args[index + 1];
}

const role = option('--role');
const logPath = option('--flink-log');
const dataDir = path.resolve(option('--data-dir') || path.join(__dirname, '../../generator/generator/data'));
const sqlitePath = option('--sqlite') ? path.resolve(option('--sqlite')) : null;
const through = option('--through');
const closedThrough = through ? Date.parse(through) : Infinity;
if ((role && !['a', 'b', 'c'].includes(role)) || (logPath && !role)
    || Number.isNaN(closedThrough)) {
  throw new Error('用法：--role a|b|c --flink-log 日志路径 [--through ISO时间]');
}
if (sqlitePath && fs.existsSync(sqlitePath)) {
  throw new Error(`SQLite 文件已存在，请换一个文件名以免覆盖：${sqlitePath}`);
}

const db = new DatabaseSync(sqlitePath || ':memory:');
db.exec('CREATE TABLE article_raw(line TEXT NOT NULL); CREATE TABLE behavior_raw(line TEXT NOT NULL);');
db.exec('BEGIN');
for (const [table, filename] of [
  ['article_raw', 'article_stream.jsonl'],
  ['behavior_raw', 'behavior_stream.jsonl'],
]) {
  const file = path.join(dataDir, filename);
  const insert = db.prepare(`INSERT INTO ${table}(line) VALUES (?)`);
  for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    if (line.trim()) insert.run(line);
  }
}
db.exec('COMMIT');
db.exec(fs.readFileSync(path.join(__dirname, 'prepare.sql'), 'utf8'));
console.log(`固定输入：${dataDir}`);
if (sqlitePath) console.log(`IDEA SQLite 数据库：${sqlitePath}`);
for (const table of ['article_raw', 'article_clean', 'behavior_raw', 'behavior_clean', 'joined']) {
  console.log(`${table}：${db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n}`);
}

function windowId(row, actual) {
  const start = actual ? Date.parse(row.window_start) : row.window_start_ms;
  const end = actual ? Date.parse(row.window_end) : row.window_end_ms;
  return `${new Date(start).toISOString()} ~ ${new Date(end).toISOString()}`;
}

function rowId(name, row, actual) {
  const window = windowId(row, actual);
  if (name === 'a') return `${window}|${row.article_id}`;
  if (name === 'b') return `${window}|${row.rank}`;
  return `${window}|${row.ip}`;
}

function differences(name, expected, actual) {
  const fields = name === 'a'
    ? ['article_id', 'title', 'category', 'click_count']
    : name === 'b'
      ? ['rank', 'category', 'score', 'top_articles']
      : ['ip', 'article_count', 'click_count', 'avg_read_duration_ms'];
  return fields.filter((field) => {
    const left = field === 'top_articles' ? JSON.parse(expected[field]) : expected[field];
    const right = actual[field];
    if (field === 'avg_read_duration_ms') return Math.abs(left - right) >= 0.001;
    return JSON.stringify(left) !== JSON.stringify(right);
  }).map((field) => `${field}: SQL=${JSON.stringify(field === 'top_articles'
    ? JSON.parse(expected[field]) : expected[field])}, Flink=${JSON.stringify(actual[field])}`);
}

let failed = false;
for (const name of role ? [role] : ['a', 'b', 'c']) {
  const rows = db.prepare(fs.readFileSync(path.join(__dirname, `role_${name}.sql`), 'utf8'))
    .all().filter((row) => row.window_end_ms <= closedThrough);
  const expected = new Map(rows.map((row) => [rowId(name, row, false), row]));
  const actual = new Map();
  if (logPath) {
    const pattern = new RegExp(`\\bROLE_${name.toUpperCase()}(?::\\d+)?>\\s*(\\{.*\\})`);
    for (const line of fs.readFileSync(path.resolve(logPath), 'utf8').split(/\r?\n/)) {
      const match = pattern.exec(line);
      if (!match) continue;
      const item = JSON.parse(match[1]);
      if (Date.parse(item.window_end) <= closedThrough) {
        // 允许迟到补算重复输出同一窗口：以日志中最后一次结果为准。
        actual.set(rowId(name, item, true), item);
      }
    }
  }
  console.log(`ROLE_${name.toUpperCase()}：SQL 基准 ${expected.size} 行`);
  const windows = new Map();
  function collect(map, side) {
    for (const [key, value] of map) {
      const window = key.slice(0, key.lastIndexOf('|'));
      if (!windows.has(window)) windows.set(window, { sql: 0, flink: 0, missing: 0, extra: 0, wrong: 0 });
      windows.get(window)[side]++;
      if (side === 'sql' && logPath) {
        if (!actual.has(key)) windows.get(window).missing++;
        else if (differences(name, value, actual.get(key)).length) windows.get(window).wrong++;
      }
      if (side === 'flink' && !expected.has(key)) windows.get(window).extra++;
    }
  }
  collect(expected, 'sql');
  if (logPath) collect(actual, 'flink');
  for (const window of [...windows.keys()].sort()) {
    const counts = windows.get(window);
    console.log(`  ${window}：SQL=${counts.sql}`
      + (logPath ? ` Flink=${counts.flink} 缺失=${counts.missing} 多出=${counts.extra} 字段差异=${counts.wrong}` : ''));
    if (counts.missing || counts.extra || counts.wrong) failed = true;
  }
  if (logPath) {
    for (const [key, value] of expected) {
      if (!actual.has(key)) console.log(`    缺失：${key}`);
      else for (const diff of differences(name, value, actual.get(key))) {
        console.log(`    ${key} ${diff}`);
      }
    }
    for (const key of actual.keys()) {
      if (!expected.has(key)) console.log(`    多出：${key}`);
    }
    if (!windows.size) console.log('  两侧均无告警；需另查作业是否完整消费固定批次。');
  }
}
db.close();
if (failed) process.exitCode = 1;

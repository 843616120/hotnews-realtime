 // 从固定生成器 JSONL 构造 SQLite 基准；Flink 日志只用于对照，不参与基准计算。
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
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
const roleNames = role === 'all' ? ['a', 'b', 'c'] : [role];
const logPath = option('--flink-log');
// snapshot 目录由 RecoverySnapshot 生成，里面保存本次运行的 MySQL 结果和 Redis 最新榜单。
const snapshotPath = option('--snapshot') ? path.resolve(option('--snapshot')) : null;
const dataDir = path.resolve(option('--data-dir') || path.join(__dirname, '../../../../generator/generator/data'));
const sqlitePath = option('--sqlite') ? path.resolve(option('--sqlite')) : null;
const through = option('--through');
const closedThrough = through ? Date.parse(through) : Infinity;
if ((role && !['a', 'b', 'c', 'all'].includes(role)) || (logPath && !role)
    || Number.isNaN(closedThrough)) {
  throw new Error('用法：--role a|b|c|all --flink-log 日志路径 [--snapshot 快照目录] [--through ISO时间]');
}
if (sqlitePath && fs.existsSync(sqlitePath)) {
  throw new Error(`SQLite 文件已存在，请换一个文件名以免覆盖：${sqlitePath}`);
}

async function forEachLine(file, callback) {
  const input = fs.createReadStream(file, { encoding: 'utf8' });
  const lines = readline.createInterface({ input, crlfDelay: Infinity });
  try {
    for await (const line of lines) await callback(line);
  } finally {
    lines.close();
    input.destroy();
  }
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8').replace(/^\uFEFF/, ''));
}

function normalizeDatabaseRow(name, row) {
  // MySQL 字段名与 Flink 日志字段只有少量差异，统一后复用同一套业务键和字段比较逻辑。
  if (name === 'a') {
    return {
      ...row,
      window_start: new Date(Number(row.window_start_ms)).toISOString(),
      window_end: new Date(Number(row.window_end_ms)).toISOString(),
    };
  }
  if (name === 'b') {
    return {
      ...row,
      rank: Number(row.rank_no),
      top_articles: typeof row.top_articles === 'string'
        ? JSON.parse(row.top_articles) : row.top_articles,
      window_start: new Date(Number(row.window_start_ms)).toISOString(),
      window_end: new Date(Number(row.window_end_ms)).toISOString(),
    };
  }
  return {
    ...row,
    alert_minute: Number(row.alert_minute_ms),
    article_ids: typeof row.article_ids === 'string'
      ? JSON.parse(row.article_ids) : row.article_ids,
    window_start: new Date(Number(row.window_start_ms)).toISOString(),
    window_end: new Date(Number(row.window_end_ms)).toISOString(),
  };
}

function windowId(row, actual) {
  const start = actual ? Date.parse(row.window_start) : row.window_start_ms;
  const end = actual ? Date.parse(row.window_end) : row.window_end_ms;
  return `${new Date(start).toISOString()} ~ ${new Date(end).toISOString()}`;
}

function rowId(name, row, actual) {
  if (name === 'c') {
    const end = actual ? (row.alert_minute ?? Date.parse(row.window_end))
      : row.window_end_ms;
    const minute = Math.floor(end / 60000) * 60000;
    return `${new Date(minute).toISOString()}|${row.ip}`;
  }
  const window = windowId(row, actual);
  if (name === 'a') return `${window}|${row.article_id}`;
  if (name === 'b') return `${window}|${row.rank}`;
}

function differences(name, expected, actual) {
  const fields = name === 'a'
    ? ['article_id', 'title', 'category', 'click_count']
    : name === 'b'
      ? ['rank', 'category', 'score', 'top_articles']
      : ['ip', 'article_count', 'click_count', 'avg_read_duration_ms', 'article_ids', 'window_start', 'window_end'];
  return fields.filter((field) => {
    if (field === 'window_start' || field === 'window_end') {
      return Date.parse(actual[field]) !== expected[`${field}_ms`];
    }
    const left = ['top_articles', 'article_ids'].includes(field)
      ? JSON.parse(expected[field]) : expected[field];
    const right = actual[field];
    if (field === 'avg_read_duration_ms') return Math.abs(left - right) >= 0.001;
    if (field === 'top_articles') {
      return !Array.isArray(right) || left.length !== right.length
        || left.some((article, index) => ['article_id', 'title', 'score']
          .some((key) => article[key] !== right[index]?.[key]));
    }
    if (field === 'article_ids') {
      return !Array.isArray(right) || left.length !== right.length
        || left.some((article, index) => article !== right[index]);
    }
    return JSON.stringify(left) !== JSON.stringify(right);
  }).map((field) => `${field}: SQL=${JSON.stringify(['top_articles', 'article_ids'].includes(field)
    ? JSON.parse(expected[field])
    : field === 'window_start' || field === 'window_end'
      ? new Date(expected[`${field}_ms`]).toISOString() : expected[field])}, Flink=${JSON.stringify(actual[field])}`);
}

function compareRows(name, expected, observed) {
  const expectedMap = new Map(expected.map(row => [rowId(name, row, false), row]));
  const observedMap = new Map(observed.map(row => [rowId(name, row, true), row]));
  let missing = 0;
  let extra = 0;
  let wrong = 0;
  const details = [];
  for (const [key, value] of expectedMap) {
    if (!observedMap.has(key)) {
      missing++;
      details.push(`缺失：${key}`);
      continue;
    }
    const diffs = differences(name, value, observedMap.get(key));
    if (diffs.length) {
      wrong++;
      details.push(`${key} ${diffs.join('; ')}`);
    }
  }
  for (const key of observedMap.keys()) {
    if (!expectedMap.has(key)) {
      extra++;
      details.push(`多出：${key}`);
    }
  }
  return {
    expected: expectedMap.size,
    observed: observedMap.size,
    missing,
    extra,
    wrong,
    passed: missing === 0 && extra === 0 && wrong === 0,
    details,
  };
}

async function main() {
  const db = new DatabaseSync(sqlitePath || ':memory:');
  db.exec('CREATE TABLE article_raw(line TEXT NOT NULL); CREATE TABLE behavior_raw(line TEXT NOT NULL);');
  db.exec('BEGIN');
  for (const [table, filename] of [
    ['article_raw', 'article_stream.jsonl'],
    ['behavior_raw', 'behavior_stream.jsonl'],
  ]) {
    const file = path.join(dataDir, filename);
    const insert = db.prepare(`INSERT INTO ${table}(line) VALUES (?)`);
    await forEachLine(file, (line) => {
      if (line.trim()) insert.run(line);
    });
  }
  db.exec('COMMIT');
  db.exec(fs.readFileSync(path.join(__dirname, '../sql/prepare.sql'), 'utf8'));
  console.log(`固定输入：${dataDir}`);
  if (sqlitePath) console.log(`IDEA SQLite 数据库：${sqlitePath}`);
  for (const table of ['article_raw', 'article_clean', 'behavior_raw', 'behavior_clean', 'joined']) {
    console.log(`${table}：${db.prepare(`SELECT COUNT(*) AS n FROM ${table}`).get().n}`);
  }

  const external = snapshotPath ? {
    mysql: {
      a: readJson(path.join(snapshotPath, 'article_alert.json')).map(row => normalizeDatabaseRow('a', row)),
      b: readJson(path.join(snapshotPath, 'category_rank.json')).map(row => normalizeDatabaseRow('b', row)),
      c: readJson(path.join(snapshotPath, 'ip_alert.json')).map(row => normalizeDatabaseRow('c', row)),
    },
    redis: readJson(path.join(snapshotPath, 'external-snapshot.json')).redis,
  } : null;
  const externalResults = {};

  let failed = false;
  for (const name of role ? roleNames : ['a', 'b', 'c']) {
  const sqlPath = path.join(__dirname, `../sql/role_${name}.sql`);
  const rows = db.prepare(fs.readFileSync(sqlPath, 'utf8'))
    .all().filter((row) => row.window_end_ms <= closedThrough);
  const expected = new Map(rows.map((row) => [rowId(name, row, false), row]));
  const actual = new Map();
    if (logPath) {
    const pattern = new RegExp(`\\bROLE_${name.toUpperCase()}(?::\\d+)?>\\s*(\\{.*\\})`);
    await forEachLine(path.resolve(logPath), (line) => {
      const match = pattern.exec(line);
      if (!match) return;
      const item = JSON.parse(match[1]);
      const end = item.retracted ? item.alert_minute + 60000 : Date.parse(item.window_end);
      if (end <= closedThrough) {
        // C 的撤销清除该 IP 在这一分钟的旧告警，其余修正按最终版本覆盖。
        const id = rowId(name, item, true);
        const previous = actual.get(id);
        if (item.retracted) {
          if (!previous || Number(item.revision ?? 0) >= Number(previous.revision ?? 0)) {
            actual.delete(id);
          }
        } else if (name === 'a') {
          // Rule A 的迟到更新与 MySQL UPSERT 一致：同一窗口保留最大计数。
          if (!previous || item.click_count >= previous.click_count) actual.set(id, item);
        } else if (!previous || Number(item.revision ?? 0) >= Number(previous.revision ?? 0)) {
          actual.set(id, item);
        }
      }
    });
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

    if (external) {
      const mysqlResult = compareRows(name, rows, external.mysql[name]);
      externalResults[`mysql_${name}`] = mysqlResult;
      console.log(`  MySQL：SQL=${mysqlResult.expected} MySQL=${mysqlResult.observed}`
        + ` 缺失=${mysqlResult.missing} 多出=${mysqlResult.extra} 字段差异=${mysqlResult.wrong}`
        + ` ${mysqlResult.passed ? 'PASS' : 'FAIL'}`);
      for (const detail of mysqlResult.details) console.log(`    MySQL ${detail}`);
      if (!mysqlResult.passed) failed = true;
    }
  }

  if (external) {
    const redisHash = external.redis?.hash || external.redis?.value || {};
    const ranking = typeof redisHash.ranking === 'string'
      ? JSON.parse(redisHash.ranking) : (redisHash.ranking || []);
    const latestWindow = Math.max(...ranking.map(row => Date.parse(row.window_start)));
    const sqlRows = db.prepare(fs.readFileSync(path.join(__dirname, '../sql/role_b.sql'), 'utf8'))
      .all().filter(row => row.window_start_ms === latestWindow);
    const redisRows = ranking.map(row => ({
      ...row,
      window_start: row.window_start,
      window_end: row.window_end,
      top_articles: row.top_articles,
    }));
    const redisResult = compareRows('b', sqlRows, redisRows);
    externalResults.redis_b = redisResult;
    console.log(`REDIS hotnews:top5:latest：SQL最新窗口=${redisResult.expected}`
      + ` Redis=${redisResult.observed} 缺失=${redisResult.missing} 多出=${redisResult.extra}`
      + ` 字段差异=${redisResult.wrong} ${redisResult.passed ? 'PASS' : 'FAIL'}`);
    for (const detail of redisResult.details) console.log(`  Redis ${detail}`);
    if (!redisResult.passed) failed = true;
  }
  db.close();
  if (failed) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});

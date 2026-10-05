const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { DatabaseSync } = require('node:sqlite');

test('固定输入的三条 SQL 按窗口计算，日志逐窗口报告差异', (t) => {
  const base = fs.realpathSync(os.tmpdir());
  const directory = fs.mkdtempSync(path.join(base, 'hotnews-role-baseline-'));
  t.after(() => {
    if (path.dirname(fs.realpathSync(directory)) !== base) {
      throw new Error('测试目录不在系统临时目录下');
    }
    fs.rmSync(directory, { recursive: true });
  });
  const id = (prefix, number, width) => `${prefix}-${String(number).padStart(width, '0')}`;
  const start = '2026-09-27T00:00:00.000Z';
  const articles = Array.from({ length: 52 }, (_, i) => ({
    event_id: id('article-event', i + 1, 8), event_type: 'publish',
    article_id: id('article', i + 1, 6), title: '测试文章', category: 'science',
    tags: ['热点'], published_at: start, event_time: start, ingest_time: start, version: 1,
  }));
  const behaviors = Array.from({ length: 1052 }, (_, i) => ({
    event_id: id('behavior-event', i + 1, 8), user_id: 'user-000001',
    article_id: id('article', i < 1001 ? 1 : i - 999, 6), action: 'click',
    ip: i < 1001 ? '127.0.0.1' : '127.0.0.2', read_duration_ms: 1000,
    event_time: i < 1001 ? '2026-09-27T00:04:59.000Z' : '2026-09-27T00:00:30.000Z',
    ingest_time: i < 1001 ? '2026-09-27T00:04:59.000Z' : '2026-09-27T00:00:30.000Z',
  }));
  fs.writeFileSync(path.join(directory, 'article_stream.jsonl'),
    articles.map(JSON.stringify).join('\n') + '\n');
  fs.writeFileSync(path.join(directory, 'behavior_stream.jsonl'),
    [...behaviors, behaviors[0], { ...behaviors[0], event_id: 'behavior-event-99999999',
      read_duration_ms: -1 }].map(JSON.stringify).join('\n') + '\n');

  const sqlite = path.join(directory, 'baseline.sqlite');
  const script = path.join(__dirname, 'verify.js');
  const result = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--sqlite', sqlite],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /behavior_clean：1053/);
  assert.match(result.stdout, /ROLE_A：SQL 基准 5 行/);
  assert.match(result.stdout, /ROLE_B：SQL 基准 1 行/);
  assert.match(result.stdout, /ROLE_C：SQL 基准 1 行/);

  const db = new DatabaseSync(sqlite);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM joined').get().n, 1052);
  const a = db.prepare(fs.readFileSync(path.join(__dirname, 'role_a.sql'), 'utf8')).all();
  assert.equal(a[0].click_count, 1001);
  assert.equal(a[0].window_end_ms, Date.parse('2026-09-27T00:05:00Z'));
  const c = db.prepare(fs.readFileSync(path.join(__dirname, 'role_c.sql'), 'utf8')).all();
  assert.equal(c[0].article_count, 51);
  assert.equal(c[0].click_count, 51);
  db.close();

  const log = path.join(directory, 'role-a.log');
  fs.writeFileSync(log, a.map((row, index) => `ROLE_A:1> ${JSON.stringify({
    article_id: row.article_id, title: row.title, category: row.category,
    click_count: index === 0 ? 999 : row.click_count,
    window_start: new Date(row.window_start_ms).toISOString(),
    window_end: new Date(row.window_end_ms).toISOString(),
  })}`).join('\n') + '\n');
  const mismatch = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--role', 'a', '--flink-log', log],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(mismatch.status, 1, mismatch.stderr);
  assert.match(mismatch.stdout, /SQL=1 Flink=1 缺失=0 多出=0 字段差异=1/);
  assert.match(mismatch.stdout, /字段差异=1/);
  assert.match(mismatch.stdout, /click_count: SQL=1001, Flink=999/);
  fs.writeFileSync(log, fs.readFileSync(log, 'utf8').split('\n')[0] + '\n');
  const missing = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--role', 'a', '--flink-log', log],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(missing.status, 1, missing.stderr);
  assert.match(missing.stdout, /缺失=1/);
});

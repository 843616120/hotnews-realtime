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
  assert.match(result.stdout, /behavior_clean：1052/);
  assert.match(result.stdout, /ROLE_A：SQL 基准 5 行/);
  assert.match(result.stdout, /ROLE_B：SQL 基准 1 行/);
  assert.match(result.stdout, /ROLE_C：SQL 基准 1 行/);

  const db = new DatabaseSync(sqlite);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM joined').get().n, 1052);
  const a = db.prepare(fs.readFileSync(path.join(__dirname, '../sql/role_a.sql'), 'utf8')).all();
  assert.equal(a[0].click_count, 1001);
  assert.equal(a[0].window_end_ms, Date.parse('2026-09-27T00:05:00Z'));
  const c = db.prepare(fs.readFileSync(path.join(__dirname, '../sql/role_c.sql'), 'utf8')).all();
  assert.equal(c[0].article_count, 51);
  assert.equal(c[0].click_count, 51);
  const b = db.prepare(fs.readFileSync(path.join(__dirname, '../sql/role_b.sql'), 'utf8')).all();
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

  const expectedA = a.map((row) => ({
    article_id: row.article_id, title: row.title, category: row.category,
    click_count: row.click_count,
    window_start: new Date(row.window_start_ms).toISOString(),
    window_end: new Date(row.window_end_ms).toISOString(),
  }));
  fs.writeFileSync(log, expectedA.map((item) => `ROLE_A:1> ${JSON.stringify(item)}`).join('\n')
    + `\nROLE_A:1> ${JSON.stringify({ ...expectedA[0], click_count: 1000 })}\n`);
  const correctedA = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--role', 'a', '--flink-log', log],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(correctedA.status, 0, correctedA.stdout + correctedA.stderr);

  const bLog = path.join(directory, 'role-b.log');
  const expectedB = {
    rank: b[0].rank, category: b[0].category, score: b[0].score,
    top_articles: JSON.parse(b[0].top_articles).map((item) => ({
      score: item.score, title: item.title, article_id: item.article_id,
    })),
    window_start: new Date(b[0].window_start_ms).toISOString(),
    window_end: new Date(b[0].window_end_ms).toISOString(),
  };
  fs.writeFileSync(bLog, `ROLE_B:1> ${JSON.stringify({ ...expectedB, score: 1 })}\n`
    + `ROLE_B:1> ${JSON.stringify(expectedB)}\n`);
  const corrected = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--role', 'b', '--flink-log', bLog],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(corrected.status, 0, corrected.stdout + corrected.stderr);
  assert.match(corrected.stdout, /字段差异=0/);

  expectedB.top_articles[0].title = '错误标题';
  fs.appendFileSync(bLog, `ROLE_B:1> ${JSON.stringify(expectedB)}\n`);
  const badTitle = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--role', 'b', '--flink-log', bLog],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(badTitle.status, 1, badTitle.stderr);
  assert.match(badTitle.stdout, /字段差异=1/);
  assert.match(badTitle.stdout, /top_articles:/);

  const cLog = path.join(directory, 'role-c.log');
  const expectedC = {
    ip: c[0].ip, article_count: c[0].article_count, click_count: c[0].click_count,
    avg_read_duration_ms: c[0].avg_read_duration_ms,
    article_ids: JSON.parse(c[0].article_ids),
    alert_minute: Math.floor(c[0].window_end_ms / 60000) * 60000,
    window_start: new Date(c[0].window_start_ms).toISOString(),
    window_end: new Date(c[0].window_end_ms).toISOString(),
  };
  fs.writeFileSync(cLog, `ROLE_C:1> ${JSON.stringify({ ...expectedC, click_count: 50 })}\n`
    + `ROLE_C:1> ${JSON.stringify(expectedC)}\n`);
  const correctedC = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--role', 'c', '--flink-log', cLog],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(correctedC.status, 0, correctedC.stdout + correctedC.stderr);
  fs.appendFileSync(cLog, `ROLE_C:1> ${JSON.stringify({
    ip: expectedC.ip, alert_minute: expectedC.alert_minute, retracted: true,
  })}\n`);
  const retractedC = spawnSync(process.execPath,
    ['--no-warnings', script, '--data-dir', directory, '--role', 'c', '--flink-log', cLog],
    { encoding: 'utf8', timeout: 20000 });
  assert.equal(retractedC.status, 1, retractedC.stderr);
  assert.match(retractedC.stdout, /缺失=1/);
});

test('按 article_id 关联，不要求首版发布事件时间早于行为', () => {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE article_raw(line TEXT NOT NULL); CREATE TABLE behavior_raw(line TEXT NOT NULL);');
  const publish = {
    event_id: 'article-event-00000001', event_type: 'publish',
    article_id: 'article-000001', title: '初版', category: 'science',
    tags: ['新闻'], published_at: '2026-09-27T00:00:00.000Z',
    event_time: '2026-09-27T00:00:00.000Z', ingest_time: '2026-09-27T00:00:00.000Z',
    version: 1,
  };
  const update = {
    ...publish, event_id: 'article-event-00000002', event_type: 'update',
    title: '更新版', version: 2,
    event_time: '2026-09-27T00:10:00.000Z', ingest_time: '2026-09-27T00:10:00.000Z',
  };
  const behavior = (eventId, time) => ({
    event_id: eventId, user_id: 'user-000001', article_id: 'article-000001',
    action: 'click', ip: '127.0.0.1', read_duration_ms: 1000,
    event_time: time, ingest_time: time,
  });
  const insertArticle = db.prepare('INSERT INTO article_raw(line) VALUES (?)');
  const insertBehavior = db.prepare('INSERT INTO behavior_raw(line) VALUES (?)');
  for (const article of [publish, update]) insertArticle.run(JSON.stringify(article));
  for (const item of [
    behavior('behavior-event-00000001', '2026-09-26T23:59:59.000Z'),
    behavior('behavior-event-00000002', '2026-09-27T00:05:00.000Z'),
    behavior('behavior-event-00000003', '2026-09-27T00:11:00.000Z'),
  ]) insertBehavior.run(JSON.stringify(item));
  db.exec(fs.readFileSync(path.join(__dirname, '../sql/prepare.sql'), 'utf8'));
  const rows = db.prepare('SELECT event_id, title FROM joined ORDER BY event_id')
    .all().map((row) => ({ event_id: row.event_id, title: row.title }));
  assert.deepEqual(rows, [
    { event_id: 'behavior-event-00000001', title: '更新版' },
    { event_id: 'behavior-event-00000002', title: '更新版' },
    { event_id: 'behavior-event-00000003', title: '更新版' },
  ]);
  db.close();
});

test('Schema ETL 按当前 Join 规则接受非空 IP，再按 event_id 去重', () => {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE article_raw(line TEXT NOT NULL); CREATE TABLE behavior_raw(line TEXT NOT NULL);');
  const time = '2026-09-27T00:00:00.000Z';
  db.prepare('INSERT INTO article_raw(line) VALUES (?)').run(JSON.stringify({
    event_id: 'article-event-00000001', event_type: 'publish',
    article_id: 'article-000001', title: '初版', category: 'science',
    tags: ['新闻'], published_at: time, event_time: time, ingest_time: time, version: 1,
  }));
  const behavior = (eventId, ip) => ({
    event_id: eventId, user_id: 'user-000001', article_id: 'article-000001',
    action: 'click', ip, read_duration_ms: 1000, event_time: time, ingest_time: time,
  });
  const insert = db.prepare('INSERT INTO behavior_raw(line) VALUES (?)');
  for (const [index, ip] of ['256.0.0.1', '127..0.1', 'a.0.0.1', '127.0.0.1',
    '127.0.0.1', '127.0.0.1'].entries()) {
    insert.run(JSON.stringify(behavior(
      index < 4 ? 'behavior-event-00000001' : `behavior-event-${String(index - 2).padStart(8, '0')}`,
      ip,
    )));
  }
  db.exec(fs.readFileSync(path.join(__dirname, '../sql/prepare.sql'), 'utf8'));
  const rows = db.prepare('SELECT event_id, ip FROM behavior_clean ORDER BY event_id').all()
    .map(({ event_id, ip }) => ({ event_id, ip }));
  assert.deepEqual(rows, [
    { event_id: 'behavior-event-00000001', ip: '256.0.0.1' },
    { event_id: 'behavior-event-00000002', ip: '127.0.0.1' },
    { event_id: 'behavior-event-00000003', ip: '127.0.0.1' },
  ]);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM joined').get().n, 3);
  db.close();
});

import fs from 'node:fs';
import path from 'node:path';

const target = path.resolve(process.argv[2]);
if (fs.existsSync(target)) throw new Error(`样本目录已存在，不覆盖：${target}`);
fs.mkdirSync(target, { recursive: true });
const origin = Date.parse('2026-09-27T00:00:00Z');
const iso = ms => new Date(origin + ms).toISOString();

function sample(role) {
  const articles = new Map(), steps = [], behaviors = [];
  let watermark = null;
  function event(article, category, action, timestamp, id) {
    if (!articles.has(article)) articles.set(article, {
      event_id: `pub-${article}`, event_type: 'publish', article_id: article,
      title: `验收文章 ${article}`, category, tags: ['验收'], version: 1,
      published_at: iso(0), event_time: iso(0), ingest_time: iso(0),
    });
    const row = {
      event_id: id || `${role}-${behaviors.length + 1}`, article_id: article,
      user_id: 'acceptance-user', action, ip: '127.0.0.1', read_duration_ms: 1000,
      event_time: iso(timestamp), ingest_time: iso(timestamp),
    };
    behaviors.push(row);
    steps.push({ kind: 'event', sequence: behaviors.length, wm_before_ms: watermark });
    return row.event_id;
  }
  function clicks(article, count, timestamp = 299999) {
    for (let i = 0; i < count; i++) event(article, 'threshold', 'click', timestamp);
  }
  function wm(ms) {
    watermark = origin + ms;
    steps.push({ kind: 'watermark', watermark_ms: watermark });
  }
  if (role === 'a') {
    clicks('count-999', 999); clicks('count-1000', 1000); clicks('count-1001', 1001);
    for (const article of ['count-999', 'count-1000']) {
      event(article, 'threshold', 'share', 299999);
      event(article, 'threshold', 'comment', 299999);
    }
    event('count-1000', 'threshold', 'click', 299999, 'a-1000');
    for (const [article, boundary] of [
      ['boundary-before', 299999], ['boundary-at', 300000], ['boundary-after', 300001],
    ]) {
      clicks(article, 1000, 270000);
      event(article, 'threshold', 'click', boundary);
    }
    clicks('late-probe', 1000, 240000);
    wm(600000);
    event('late-probe', 'threshold', 'click', 240000);
    // 清理时间为 end-1ms+allowedLateness，精确测试前 1ms、边界及后 1ms。
    wm(3299998); event('late-probe', 'threshold', 'click', 240000);
    wm(3299999); event('late-probe', 'threshold', 'click', 240000);
    wm(3300000); event('late-probe', 'threshold', 'click', 240000);
    wm(3539999); event('late-probe', 'threshold', 'click', 240000);
    wm(3600000);
  } else {
    for (let i = 1; i <= 6; i++) event(`alpha-${i}`, 'alpha', 'click', 599999);
    const duplicate = event('alpha-1', 'alpha', 'share', 599999);
    event('alpha-1', 'alpha', 'comment', 599999);
    event('alpha-1', 'alpha', 'share', 599999, duplicate);
    for (const category of ['zeta', 'epsilon', 'delta', 'charlie', 'bravo']) {
      for (let i = 1; i <= 2; i++) event(`${category}-${i}`, category, 'click', 599999);
    }
    event('boundary', 'next', 'click', 600000);
    event('boundary', 'next', 'share', 600001);
    wm(1200000);
    // 同窗口晚到使 bravo 超过 alpha，验证完整榜单修订。
    for (let i = 0; i < 7; i++) event('bravo-1', 'bravo', 'comment', 599999);
    wm(4499998); event('bravo-1', 'bravo', 'share', 599999);
    wm(4499999); event('bravo-1', 'bravo', 'click', 599999);
    wm(4500000); event('bravo-1', 'bravo', 'click', 599999);
    wm(4800000);
  }
  const dir = path.join(target, `role-${role}`);
  fs.mkdirSync(dir);
  fs.writeFileSync(path.join(dir, 'article_stream.jsonl'),
    [...articles.values()].map(JSON.stringify).join('\n') + '\n', { flag: 'wx' });
  fs.writeFileSync(path.join(dir, 'behavior_stream.jsonl'),
    behaviors.map(JSON.stringify).join('\n') + '\n', { flag: 'wx' });
  fs.writeFileSync(path.join(dir, 'schedule.json'), JSON.stringify(steps, null, 2), { flag: 'wx' });
  console.log(`${role}: articles=${articles.size}, behavior_raw=${behaviors.length}`);
}
sample('a');
sample('b');

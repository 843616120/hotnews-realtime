import fs from 'node:fs';
import path from 'node:path';
const ruleDirectory = path.resolve(process.argv[2]);
const sinkDirectory = path.resolve(process.argv[3]);
const load = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const rule = load(path.join(ruleDirectory, 'verification.json'));
const mysql = load(path.join(sinkDirectory, 'mysql-test-results.json'));
const checks = load(path.join(sinkDirectory, 'sink-checks.json'));
const differences = [];
const articles = values => values.map(({ article_id, title, score }) => ({ article_id, title, score }));
for (const role of ['a', 'b']) {
  const key = row => `${row.window_start_ms}|${role === 'a' ? row.article_id : row.rank}`;
  const fields = role === 'a' ? ['window_end_ms', 'title', 'category', 'click_count']
    : ['window_end_ms', 'category', 'score', 'revision', 'top_articles'];
  const expected = new Map(rule.evidence[role].sqlOnline.map(row => [key(row), row]));
  const actual = new Map(mysql[role].map(row => {
    if (role === 'b') { row.rank = row.rank_no; row.top_articles = JSON.parse(row.top_articles); }
    return [key(row), row];
  }));
  for (const [id, row] of expected) {
    if (!actual.has(id)) differences.push({ role, key: id, type: 'missing' });
    else for (const field of fields) {
      const left = field === 'top_articles' ? articles(row[field]) : row[field];
      const right = field === 'top_articles' ? articles(actual.get(id)[field]) : actual.get(id)[field];
      if (JSON.stringify(left) !== JSON.stringify(right)) differences.push({ role, key: id, field, expected: left, actual: right });
    }
  }
  for (const id of actual.keys()) if (!expected.has(id)) differences.push({ role, key: id, type: 'extra' });
  checks.push({ name: `${role.toUpperCase()} MySQL temporary table vs independent SQL`,
    expected: 0, actual: differences.filter(row => row.role === role).length,
    passed: !differences.some(row => row.role === role) });
}
fs.writeFileSync(path.join(sinkDirectory, 'sink-verification.json'),
  JSON.stringify({ checks, differences }, null, 2), { flag: 'wx' });
for (const check of checks) console.log(`${check.passed ? 'PASS' : 'FAIL'} ${check.name}`);
if (checks.some(check => !check.passed)) process.exitCode = 1;

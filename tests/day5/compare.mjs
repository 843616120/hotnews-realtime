import { readFileSync } from "node:fs";

const [beforePath, afterPath] = process.argv.slice(2);
if (!beforePath || !afterPath) {
  throw new Error("Usage: node tests/day5/compare.mjs <before.json> <after.json>");
}
const before = JSON.parse(readFileSync(beforePath, "utf8"));
const after = JSON.parse(readFileSync(afterPath, "utf8"));
const failures = [];
const check = (condition, message) => { if (!condition) failures.push(message); };

function rows(array, keyColumns) {
  return new Map(array.map(row => [keyColumns.map(i => row[i]).join("/"), row]));
}

check(after.mysql.counts[0] === before.mysql.counts[0],
  `clean_behavior count changed: ${before.mysql.counts[0]} -> ${after.mysql.counts[0]}`);
for (const [name, keyColumns, numericColumns] of [
  ["article_alert", [0, 1], [2]],
  ["category_rank", [0, 1], [4]],
]) {
  const earlier = rows(name === "article_alert" ? before.mysql.alerts : before.mysql.ranks, keyColumns);
  const later = rows(name === "article_alert" ? after.mysql.alerts : after.mysql.ranks, keyColumns);
  for (const [key, oldRow] of earlier) {
    const newRow = later.get(key);
    check(Boolean(newRow), `${name} missing ${key}`);
    if (!newRow) continue;
    for (const index of numericColumns) {
      check(BigInt(newRow[index]) >= BigInt(oldRow[index]),
        `${name} ${key} column ${index} went backwards: ${oldRow[index]} -> ${newRow[index]}`);
    }
    if (name === "category_rank" && oldRow[4] === newRow[4]) {
      check(oldRow.slice(2, 4).join("/") === newRow.slice(2, 4).join("/"),
        `category_rank ${key} changed without a revision increase`);
    }
  }
}
if (before.redis.windowStartMs !== "(nil)" && after.redis.windowStartMs !== "(nil)") {
  const oldWindow = BigInt(before.redis.windowStartMs);
  const newWindow = BigInt(after.redis.windowStartMs);
  check(newWindow >= oldWindow, "Redis latest window went backwards");
  check(after.redis.rankingLength === 5, "Redis latest ranking is not a complete Top 5");
  if (newWindow === oldWindow) {
    check(BigInt(after.redis.revision) >= BigInt(before.redis.revision),
      "Redis revision went backwards");
    if (after.redis.revision === before.redis.revision &&
        before.redis.rankingSemanticSha256 && after.redis.rankingSemanticSha256) {
      check(after.redis.rankingSemanticSha256 === before.redis.rankingSemanticSha256,
        "Redis business ranking changed at the same window and revision");
    }
  }
} else {
  check(false, "Redis ranking absent at one capture; cannot verify monotonicity");
}

console.log(JSON.stringify({
  before: before.stage, after: after.stage, checkedArticleAlerts: before.mysql.alerts.length,
  checkedCategoryRanks: before.mysql.ranks.length, failures,
}, null, 2));
if (failures.length) process.exitCode = 1;

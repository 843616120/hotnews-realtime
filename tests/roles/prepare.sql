-- 两张 raw 表仅由固定生成器 JSONL 导入，不读取 Flink 的任何输出。
-- 离线清洗用于独立构造业务基线；Watermark 和跨 Topic 到达顺序不由 SQL 模拟。
CREATE TABLE article_clean AS
WITH parsed AS (
    SELECT rowid AS sequence, CASE WHEN json_valid(line) THEN line ELSE '{}' END AS doc
    FROM article_raw
), fields AS (
    SELECT sequence, doc,
           json_extract(doc, '$.event_id') AS event_id,
           json_extract(doc, '$.event_type') AS event_type,
           json_extract(doc, '$.article_id') AS article_id,
           json_extract(doc, '$.title') AS title,
           json_extract(doc, '$.category') AS category,
           json_extract(doc, '$.tags') AS tags,
           json_extract(doc, '$.version') AS version,
           json_extract(doc, '$.published_at') AS published_at,
           json_extract(doc, '$.event_time') AS event_time,
           json_extract(doc, '$.ingest_time') AS ingest_time
    FROM parsed
)
SELECT sequence, article_id, event_type, title, category, version,
       unixepoch(event_time) * 1000 + CAST(substr(event_time, 21, 3) AS INTEGER) AS event_ms
FROM fields
WHERE json_type(doc, '$.event_id') = 'text'
  AND event_id GLOB 'article-event-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
  AND event_type IN ('publish', 'update')
  AND json_type(doc, '$.article_id') = 'text'
  AND article_id GLOB 'article-[0-9][0-9][0-9][0-9][0-9][0-9]'
  AND json_type(doc, '$.title') = 'text' AND length(title) BETWEEN 1 AND 200
  AND json_type(doc, '$.category') = 'text' AND length(category) BETWEEN 1 AND 64
  AND json_type(doc, '$.tags') = 'array'
  AND json_array_length(tags) BETWEEN 1 AND 10
  AND (SELECT COUNT(*) FROM json_each(fields.tags)
       WHERE type != 'text' OR length(value) NOT BETWEEN 1 AND 32) = 0
  AND (SELECT COUNT(DISTINCT value) FROM json_each(fields.tags)) = json_array_length(tags)
  AND json_type(doc, '$.version') = 'integer' AND version >= 1
  AND json_type(doc, '$.published_at') = 'text' AND unixepoch(published_at) IS NOT NULL
  AND json_type(doc, '$.event_time') = 'text' AND unixepoch(event_time) IS NOT NULL
  AND json_type(doc, '$.ingest_time') = 'text' AND unixepoch(ingest_time) IS NOT NULL
  AND unixepoch(event_time) <= unixepoch(ingest_time) + 7200;

CREATE TABLE behavior_clean AS
WITH RECURSIVE parsed AS (
    SELECT rowid AS sequence, CASE WHEN json_valid(line) THEN line ELSE '{}' END AS doc
    FROM behavior_raw
), fields AS (
    SELECT sequence, doc,
           json_extract(doc, '$.event_id') AS event_id,
           json_extract(doc, '$.user_id') AS user_id,
           json_extract(doc, '$.article_id') AS article_id,
           json_extract(doc, '$.action') AS action,
           json_extract(doc, '$.ip') AS ip,
           json_extract(doc, '$.read_duration_ms') AS read_duration_ms,
           json_extract(doc, '$.event_time') AS event_time,
           json_extract(doc, '$.ingest_time') AS ingest_time
    FROM parsed
), ip_octets AS (
    SELECT sequence, substr(ip, 1, instr(ip, '.') - 1) AS octet,
           substr(ip, instr(ip, '.') + 1) AS remaining, 1 AS position
    FROM fields
    WHERE json_type(doc, '$.ip') = 'text' AND instr(ip, '.') > 0
    UNION ALL
    SELECT sequence,
           CASE WHEN instr(remaining, '.') > 0
                THEN substr(remaining, 1, instr(remaining, '.') - 1)
                ELSE remaining END,
           CASE WHEN instr(remaining, '.') > 0
                THEN substr(remaining, instr(remaining, '.') + 1)
                ELSE '' END,
           position + 1
    FROM ip_octets
    WHERE position < 4 AND remaining <> ''
), valid AS (
SELECT sequence, event_id, article_id, action, ip, read_duration_ms,
       unixepoch(event_time) * 1000 + CAST(substr(event_time, 21, 3) AS INTEGER) AS event_ms
FROM fields
WHERE json_type(doc, '$.event_id') = 'text'
  AND event_id GLOB 'behavior-event-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
  AND json_type(doc, '$.user_id') = 'text'
  AND user_id GLOB 'user-[0-9][0-9][0-9][0-9][0-9][0-9]'
  AND json_type(doc, '$.article_id') = 'text'
  AND article_id GLOB 'article-[0-9][0-9][0-9][0-9][0-9][0-9]'
  AND action IN ('click', 'share', 'comment')
  AND json_type(doc, '$.ip') = 'text'
  AND length(ip) - length(replace(ip, '.', '')) = 3
  AND EXISTS (
      SELECT 1 FROM ip_octets WHERE ip_octets.sequence = fields.sequence
      GROUP BY ip_octets.sequence
      HAVING COUNT(*) = 4
         AND MIN(CASE WHEN length(octet) BETWEEN 1 AND 3
                           AND octet NOT GLOB '*[^0-9]*'
                           AND CAST(octet AS INTEGER) <= 255
                      THEN 1 ELSE 0 END) = 1
  )
  AND json_type(doc, '$.read_duration_ms') = 'integer'
  AND read_duration_ms BETWEEN 0 AND 86400000
  AND json_type(doc, '$.event_time') = 'text' AND unixepoch(event_time) IS NOT NULL
  AND json_type(doc, '$.ingest_time') = 'text' AND unixepoch(ingest_time) IS NOT NULL
  AND unixepoch(event_time) <= unixepoch(ingest_time) + 7200
)
-- 先按 Schema 清洗再去重，避免脏副本挤掉同 ID 的合法行为。
SELECT sequence, event_id, article_id, action, ip, read_duration_ms, event_ms
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY sequence) AS event_rank
    FROM valid
)
WHERE event_rank = 1;

-- 最初的 version=1 publish 决定行为有效时间；更新版本只决定富化的文章信息。
CREATE TABLE joined AS
WITH articles AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY article_id ORDER BY version DESC, event_ms DESC, sequence DESC
    ) AS article_rank
    FROM article_clean
), initial_publication AS (
    SELECT article_id, MIN(event_ms) AS first_event_ms
    FROM article_clean
    WHERE event_type = 'publish' AND version = 1
    GROUP BY article_id
)
SELECT b.event_id, b.article_id, b.action, b.ip, b.read_duration_ms, b.event_ms,
       a.title, a.category, a.version AS article_version
FROM behavior_clean b JOIN articles a ON a.article_id = b.article_id
JOIN initial_publication p ON p.article_id = b.article_id
WHERE a.article_rank = 1 AND b.event_ms >= p.first_event_ms;

CREATE INDEX joined_article_time ON joined(article_id, event_ms);
CREATE INDEX joined_ip_time ON joined(ip, event_ms);

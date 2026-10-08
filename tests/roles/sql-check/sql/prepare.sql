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
  AND length(trim(event_id)) > 0
  AND event_type IN ('publish', 'update')
  AND json_type(doc, '$.article_id') = 'text'
  AND length(trim(article_id)) > 0
  AND json_type(doc, '$.title') = 'text' AND length(trim(title)) > 0
  AND json_type(doc, '$.category') = 'text' AND length(trim(category)) > 0
  AND json_type(doc, '$.tags') = 'array'
  AND json_array_length(tags) > 0
  AND json_type(doc, '$.version') = 'integer' AND version >= 1
  AND json_type(doc, '$.published_at') = 'text' AND unixepoch(published_at) IS NOT NULL
  AND json_type(doc, '$.event_time') = 'text' AND unixepoch(event_time) IS NOT NULL
  AND json_type(doc, '$.ingest_time') = 'text' AND unixepoch(ingest_time) IS NOT NULL
  AND unixepoch(event_time) <= unixepoch(ingest_time) + 7200;

CREATE TABLE behavior_clean AS
WITH parsed AS (
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
), valid AS (
SELECT sequence, event_id, article_id, action, ip, read_duration_ms,
       unixepoch(event_time) * 1000 + CAST(substr(event_time, 21, 3) AS INTEGER) AS event_ms
FROM fields
WHERE json_type(doc, '$.event_id') = 'text'
  AND length(trim(event_id)) > 0
  AND json_type(doc, '$.user_id') = 'text'
  AND length(trim(user_id)) > 0
  AND json_type(doc, '$.article_id') = 'text'
  AND length(trim(article_id)) > 0
  AND action IN ('click', 'share', 'comment')
  AND json_type(doc, '$.ip') = 'text'
  AND length(trim(ip)) > 0
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

-- 现有 Join 按 article_id 关联，不要求先读到 version=1 的发布事件。
-- 固定样本只有发布事件；若以后加入文章更新，SQL 取最高版本，不模拟跨 Topic 的真实到达顺序。
CREATE TABLE joined AS
WITH articles AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY article_id ORDER BY version DESC, event_ms DESC, sequence DESC
    ) AS article_rank
    FROM article_clean
)
SELECT b.event_id, b.article_id, b.action, b.ip, b.read_duration_ms, b.event_ms,
       a.title, a.category, a.version AS article_version
FROM behavior_clean b JOIN articles a ON a.article_id = b.article_id
WHERE a.article_rank = 1;

CREATE INDEX joined_article_time ON joined(article_id, event_ms);
CREATE INDEX joined_ip_time ON joined(ip, event_ms);

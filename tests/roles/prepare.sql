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
SELECT sequence, article_id, title, category, version,
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
)
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
  AND json_type(doc, '$.read_duration_ms') = 'integer'
  AND read_duration_ms BETWEEN 0 AND 86400000
  AND json_type(doc, '$.event_time') = 'text' AND unixepoch(event_time) IS NOT NULL
  AND json_type(doc, '$.ingest_time') = 'text' AND unixepoch(ingest_time) IS NOT NULL
  AND unixepoch(event_time) <= unixepoch(ingest_time) + 7200;

-- 固定数据只有 publish；排序仍保留同一文章多个版本的离线候选逻辑。
CREATE TABLE joined AS
WITH articles AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY article_id ORDER BY version DESC, event_ms DESC, sequence DESC
    ) AS article_rank
    FROM article_clean
), behaviors AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY sequence) AS behavior_rank
    FROM behavior_clean
)
SELECT b.event_id, b.article_id, b.action, b.ip, b.read_duration_ms, b.event_ms,
       a.title, a.category, a.version AS article_version
FROM behaviors b JOIN articles a ON a.article_id = b.article_id
WHERE b.behavior_rank = 1 AND a.article_rank = 1 AND b.event_ms >= a.event_ms;

CREATE INDEX joined_article_time ON joined(article_id, event_ms);
CREATE INDEX joined_ip_time ON joined(ip, event_ms);

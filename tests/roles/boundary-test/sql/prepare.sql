-- 只从原始 JSONL 清洗、去重和关联，不读取规则输出。
-- 使用完整时间解析，避免截取毫秒字符串误读不带小数或带时区的时间。
CREATE TABLE article_clean AS
WITH parsed AS (
    SELECT rowid AS sequence, CASE WHEN json_valid(line) THEN line ELSE '{}' END AS doc
    FROM article_raw
), fields AS (
    SELECT sequence, doc, json_extract(doc, '$.article_id') AS article_id,
           json_extract(doc, '$.title') AS title, json_extract(doc, '$.category') AS category,
           json_extract(doc, '$.version') AS version,
           CAST(round(unixepoch(json_extract(doc, '$.event_time'), 'subsec') * 1000) AS INTEGER) AS event_ms,
           CAST(round(unixepoch(json_extract(doc, '$.ingest_time'), 'subsec') * 1000) AS INTEGER) AS ingest_ms
    FROM parsed
)
SELECT sequence, article_id, title, category, version, event_ms
FROM fields
WHERE json_type(doc, '$.event_id') = 'text' AND length(trim(json_extract(doc, '$.event_id'))) > 0
  AND json_type(doc, '$.article_id') = 'text' AND length(trim(article_id)) > 0
  AND json_type(doc, '$.title') = 'text' AND length(trim(title)) > 0
  AND json_type(doc, '$.category') = 'text' AND length(trim(category)) > 0
  AND json_extract(doc, '$.event_type') IN ('publish', 'update')
  AND json_type(doc, '$.tags') = 'array' AND json_array_length(json_extract(doc, '$.tags')) > 0
  AND json_type(doc, '$.version') = 'integer' AND version BETWEEN 1 AND 2147483647
  AND json_type(doc, '$.published_at') = 'text'
  AND unixepoch(json_extract(doc, '$.published_at'), 'subsec') IS NOT NULL
  AND json_type(doc, '$.event_time') = 'text' AND event_ms IS NOT NULL
  AND json_type(doc, '$.ingest_time') = 'text' AND ingest_ms IS NOT NULL
  AND event_ms <= ingest_ms + 7200000;

CREATE TABLE behavior_clean AS
WITH parsed AS (
    SELECT rowid AS sequence, CASE WHEN json_valid(line) THEN line ELSE '{}' END AS doc
    FROM behavior_raw
), fields AS (
    SELECT sequence, doc, json_extract(doc, '$.event_id') AS event_id,
           json_extract(doc, '$.article_id') AS article_id,
           json_extract(doc, '$.action') AS action,
           CAST(round(unixepoch(json_extract(doc, '$.event_time'), 'subsec') * 1000) AS INTEGER) AS event_ms,
           CAST(round(unixepoch(json_extract(doc, '$.ingest_time'), 'subsec') * 1000) AS INTEGER) AS ingest_ms
    FROM parsed
), valid AS (
    SELECT * FROM fields
    WHERE json_type(doc, '$.event_id') = 'text' AND length(trim(event_id)) > 0
      AND json_type(doc, '$.article_id') = 'text' AND length(trim(article_id)) > 0
      AND json_type(doc, '$.user_id') = 'text' AND length(trim(json_extract(doc, '$.user_id'))) > 0
      AND json_type(doc, '$.ip') = 'text' AND length(trim(json_extract(doc, '$.ip'))) > 0
      AND action IN ('click', 'share', 'comment')
      AND json_type(doc, '$.read_duration_ms') = 'integer'
      AND json_extract(doc, '$.read_duration_ms') BETWEEN 0 AND 86400000
      AND json_type(doc, '$.event_time') = 'text' AND event_ms IS NOT NULL
      AND json_type(doc, '$.ingest_time') = 'text' AND ingest_ms IS NOT NULL
      AND event_ms <= ingest_ms + 7200000
)
SELECT sequence, event_id, article_id, action, event_ms
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY sequence) AS duplicate_no
    FROM valid
) WHERE duplicate_no = 1;

-- 样本只含单版文章。多版本生产输入不能用离线最高版本模拟真实到达顺序。
CREATE TABLE joined AS
WITH articles AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY article_id ORDER BY version DESC, event_ms DESC, sequence DESC
    ) AS version_no FROM article_clean
)
SELECT b.*, a.title, a.category, a.version AS article_version
FROM behavior_clean b JOIN articles a USING(article_id)
WHERE a.version_no = 1;

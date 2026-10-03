-- verify.py loads the JSONL lines into article_raw, behavior_raw and flink_output.
-- This baseline covers the fixed test1_data dataset; the Java ETL remains the schema authority.
CREATE TEMP VIEW article_fields AS
SELECT line,
       json_valid(line) AS json_ok,
       CASE WHEN json_valid(line) THEN line ELSE '{}' END AS doc
FROM article_raw;

CREATE TEMP VIEW articles AS
SELECT line, json_ok,
       json_extract(doc, '$.article_id') AS article_id,
       json_extract(doc, '$.title') AS title,
       json_extract(doc, '$.category') AS category,
       json_extract(doc, '$.tags') AS tags,
       ROUND((julianday(json_extract(doc, '$.event_time')) - 2440587.5) * 86400000) AS event_ms,
       CASE WHEN json_ok = 1
                 AND json_type(doc, '$.article_id') = 'text'
                 AND json_type(doc, '$.title') = 'text'
                 AND json_type(doc, '$.category') = 'text'
                 AND json_type(doc, '$.tags') = 'array'
                 AND julianday(json_extract(doc, '$.event_time')) IS NOT NULL
            THEN 1 ELSE 0 END AS valid
FROM article_fields;

CREATE TEMP VIEW behavior_fields AS
SELECT line,
       json_valid(line) AS json_ok,
       CASE WHEN json_valid(line) THEN line ELSE '{}' END AS doc
FROM behavior_raw;

CREATE TEMP VIEW behaviors AS
SELECT line, json_ok,
       json_extract(doc, '$.event_id') AS event_id,
       json_extract(doc, '$.article_id') AS article_id,
       json_extract(doc, '$.read_duration_ms') AS duration_ms,
       ROUND((julianday(json_extract(doc, '$.event_time')) - 2440587.5) * 86400000) AS event_ms,
       ROUND((julianday(json_extract(doc, '$.ingest_time')) - 2440587.5) * 86400000) AS ingest_ms,
       CASE
           WHEN json_ok = 0 THEN 'invalid_json'
           WHEN json_type(doc, '$.event_id') != 'text'
                OR json_type(doc, '$.article_id') != 'text'
                OR json_extract(doc, '$.article_id') = ''
                OR json_type(doc, '$.read_duration_ms') != 'integer'
                OR julianday(json_extract(doc, '$.event_time')) IS NULL
                OR julianday(json_extract(doc, '$.ingest_time')) IS NULL
               THEN 'missing_or_invalid_field'
           WHEN json_extract(doc, '$.read_duration_ms') < 0
                OR json_extract(doc, '$.read_duration_ms') > 86400000
               THEN 'invalid_duration'
           WHEN julianday(json_extract(doc, '$.event_time'))
                    > julianday(json_extract(doc, '$.ingest_time')) + 2.0 / 24
               THEN 'future_event_time'
           ELSE NULL
       END AS dirty_reason
FROM behavior_fields;

CREATE TEMP VIEW valid_articles AS SELECT * FROM articles WHERE valid = 1;
CREATE TEMP VIEW valid_behaviors AS SELECT * FROM behaviors WHERE dirty_reason IS NULL;

-- Candidates assume the article is available before its behavior times out. Runtime
-- late/unmatched results also depend on Kafka ordering and watermark progress.
CREATE TEMP VIEW expected_join AS
SELECT b.event_id, b.line AS behavior_line, a.article_id,
       a.title, a.category, a.tags, b.event_ms - a.event_ms AS delta_ms
FROM valid_behaviors b
JOIN valid_articles a ON b.article_id = a.article_id
WHERE b.event_ms >= a.event_ms;

CREATE TEMP VIEW day2_summary AS
SELECT 'articles_input' AS metric, COUNT(*) AS n FROM article_raw
UNION ALL SELECT 'behaviors_input', COUNT(*) FROM behavior_raw
UNION ALL SELECT 'articles_dirty', COUNT(*) FROM articles WHERE valid = 0
UNION ALL SELECT 'behaviors_dirty', COUNT(*) FROM behaviors WHERE dirty_reason IS NOT NULL
UNION ALL SELECT 'dirty_invalid_duration', COUNT(*) FROM behaviors WHERE dirty_reason = 'invalid_duration'
UNION ALL SELECT 'dirty_future_event_time', COUNT(*) FROM behaviors WHERE dirty_reason = 'future_event_time'
UNION ALL SELECT 'behavior_duplicate_event_id', COUNT(event_id) - COUNT(DISTINCT event_id) FROM behaviors
UNION ALL SELECT 'old_interval_30s_pairs', COUNT(*) FROM expected_join WHERE delta_ms BETWEEN -30000 AND 30000
UNION ALL SELECT 'state_join_candidates', COUNT(*) FROM expected_join
UNION ALL SELECT 'no_article_candidates', COUNT(*) FROM valid_behaviors b
    WHERE NOT EXISTS (SELECT 1 FROM valid_articles a WHERE a.article_id = b.article_id AND a.event_ms <= b.event_ms);

CREATE TEMP VIEW expected_counts AS
SELECT event_id, COUNT(*) AS n FROM expected_join GROUP BY event_id;
CREATE TEMP VIEW actual_counts AS
SELECT json_extract(CASE WHEN json_valid(line) THEN line ELSE '{}' END, '$.event_id') AS event_id,
       COUNT(*) AS n
FROM flink_output WHERE tag = 'JOINED' AND json_valid(line)
GROUP BY json_extract(CASE WHEN json_valid(line) THEN line ELSE '{}' END, '$.event_id');

CREATE TEMP VIEW day2_comparison AS
SELECT 'joined_actual' AS metric, COUNT(*) AS n FROM flink_output WHERE tag = 'JOINED'
UNION ALL SELECT 'joined_missing', COALESCE(SUM(max(e.n - COALESCE(a.n, 0), 0)), 0)
    FROM expected_counts e LEFT JOIN actual_counts a ON a.event_id = e.event_id
UNION ALL SELECT 'joined_unexpected', COALESCE(SUM(max(a.n - COALESCE(e.n, 0), 0)), 0)
    FROM actual_counts a LEFT JOIN expected_counts e ON e.event_id = a.event_id
UNION ALL SELECT 'joined_wrong_article_fields', COUNT(*) FROM flink_output f
    JOIN valid_articles a ON a.article_id =
        json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.article_id')
    WHERE f.tag = 'JOINED' AND json_valid(f.line)
      AND (json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.title') IS NOT a.title
        OR json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.category') IS NOT a.category
        OR json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.tags') IS NOT a.tags)
UNION ALL SELECT 'joined_dirty_fields', COUNT(*) FROM flink_output f
    WHERE f.tag = 'JOINED'
      AND (NOT json_valid(f.line)
        OR json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.article_id') IS NULL
        OR json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.read_duration_ms') < 0
        OR julianday(json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.event_time'))
           > julianday(json_extract(CASE WHEN json_valid(f.line) THEN f.line ELSE '{}' END, '$.ingest_time')) + 2.0 / 24)
UNION ALL SELECT 'dirty_actual', COUNT(*) FROM flink_output WHERE tag IN ('DIRTY_ARTICLE', 'DIRTY_BEHAVIOR')
UNION ALL SELECT 'dirty_missing', COALESCE(SUM(max(e.n - COALESCE(a.n, 0), 0)), 0)
    FROM (SELECT line, COUNT(*) AS n FROM behaviors WHERE dirty_reason IS NOT NULL GROUP BY line) e
    LEFT JOIN (SELECT line, COUNT(*) AS n FROM flink_output WHERE tag = 'DIRTY_BEHAVIOR' GROUP BY line) a
    ON a.line = e.line
UNION ALL SELECT 'dirty_unexpected', COALESCE(SUM(max(a.n - COALESCE(e.n, 0), 0)), 0)
    FROM (SELECT line, COUNT(*) AS n FROM flink_output WHERE tag = 'DIRTY_BEHAVIOR' GROUP BY line) a
    LEFT JOIN (SELECT line, COUNT(*) AS n FROM behaviors WHERE dirty_reason IS NOT NULL GROUP BY line) e
    ON a.line = e.line
UNION ALL SELECT 'late_actual', COUNT(*) FROM flink_output WHERE tag = 'LATE_DATA'
UNION ALL SELECT 'unmatched_actual', COUNT(*) FROM flink_output WHERE tag = 'UNMATCHED_BEHAVIOR';

-- 独立基准 B：固定输入按十分钟滚动窗口统计所有行为，分类排名及类内前五篇。
-- joined 已按 event_id 去重并取文章最新版本；SQL 不模拟 Kafka 位点和 Join Watermark。
-- 用 (window_start_ms, rank) 逐行对照，不把不同窗口合并成最终榜单。
WITH article_events AS (
    SELECT *, CAST(event_ms / 600000 AS INTEGER) * 600000 AS window_start_ms,
           ROW_NUMBER() OVER (
               PARTITION BY category, article_id, CAST(event_ms / 600000 AS INTEGER)
               ORDER BY article_version DESC, event_ms DESC
           ) AS latest
    FROM joined
), article_scores AS (
    SELECT window_start_ms, category, article_id,
           MAX(CASE WHEN latest = 1 THEN title END) AS title, COUNT(*) AS score
    FROM article_events
    GROUP BY window_start_ms, category, article_id
), category_scores AS (
    SELECT window_start_ms, category, SUM(score) AS score
    FROM article_scores GROUP BY window_start_ms, category
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY window_start_ms ORDER BY score DESC, category ASC
    ) AS rank
    FROM category_scores
)
SELECT r.window_start_ms, r.window_start_ms + 600000 AS window_end_ms,
       r.rank, r.category, r.score,
       (SELECT json_group_array(json_object(
           'article_id', t.article_id, 'title', t.title, 'score', t.score))
        FROM (
            SELECT article_id, title, score FROM article_scores a
            WHERE a.window_start_ms = r.window_start_ms AND a.category = r.category
            ORDER BY score DESC, article_id ASC LIMIT 5
        ) t) AS top_articles
FROM ranked r
WHERE r.rank <= 5
ORDER BY r.window_start_ms, r.rank;

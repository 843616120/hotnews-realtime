WITH accepted AS (
    SELECT j.*, CAST(event_ms / 600000 AS INTEGER) * 600000 AS window_start_ms
    FROM joined j LEFT JOIN schedule s ON s.sequence = j.sequence
    WHERE :online = 0 OR s.wm_before_ms IS NULL
       OR s.wm_before_ms < CAST(event_ms / 600000 AS INTEGER) * 600000 + 600000 - 1 + 3900000
), articles AS (
    SELECT window_start_ms, category, article_id, MAX(title) AS title, COUNT(*) AS score
    FROM accepted GROUP BY window_start_ms, category, article_id
), categories AS (
    SELECT window_start_ms, category, SUM(score) AS score
    FROM articles GROUP BY window_start_ms, category
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start_ms ORDER BY score DESC, category) AS rank,
           SUM(score) OVER (PARTITION BY window_start_ms) AS revision
    FROM categories
)
SELECT window_start_ms, window_start_ms + 600000 AS window_end_ms, rank, category, score, revision,
       (SELECT json_group_array(json_object('article_id', article_id, 'title', title, 'score', score))
        FROM (SELECT article_id, title, score FROM articles a
              WHERE a.window_start_ms = r.window_start_ms AND a.category = r.category
              ORDER BY score DESC, article_id LIMIT 5)) AS top_articles
FROM ranked r WHERE rank <= 5 ORDER BY window_start_ms, rank;

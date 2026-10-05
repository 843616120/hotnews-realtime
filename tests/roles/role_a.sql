-- 独立基准 A：固定输入离线关联后的点击流；按文章统计五分钟窗口，每分钟滑动。
-- 每行是一篇文章在一个 [window_start_ms, window_end_ms) 内的告警。
WITH offsets(n) AS (VALUES (0), (1), (2), (3), (4)),
windows AS (
    SELECT j.article_id, j.title, j.category, j.article_version, j.event_ms,
           (CAST(j.event_ms / 60000 AS INTEGER) - offsets.n) * 60000 AS window_start_ms
    FROM joined j CROSS JOIN offsets
    WHERE j.action = 'click'
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY article_id, window_start_ms
        ORDER BY article_version DESC, event_ms DESC
    ) AS latest
    FROM windows
)
SELECT article_id,
       MAX(CASE WHEN latest = 1 THEN title END) AS title,
       MAX(CASE WHEN latest = 1 THEN category END) AS category,
       COUNT(*) AS click_count, window_start_ms,
       window_start_ms + 300000 AS window_end_ms
FROM ranked
GROUP BY article_id, window_start_ms
HAVING COUNT(*) > 1000
ORDER BY window_start_ms, article_id;

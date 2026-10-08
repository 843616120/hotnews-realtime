-- wm_before_ms 来自测试源的显式 Watermark 日程，不来自 Flink 输出。
-- A 要逐个重叠窗口判断保留期；只排除 ROLE_A_LATE 整条事件会遗漏部分窗口超期。
WITH offsets(n) AS (VALUES(0),(1),(2),(3),(4)), windows AS (
    SELECT j.*, (CAST(event_ms / 60000 AS INTEGER) - n) * 60000 AS window_start_ms,
           s.wm_before_ms
    FROM joined j CROSS JOIN offsets
    LEFT JOIN schedule s ON s.sequence = j.sequence
    WHERE action = 'click'
), accepted AS (
    SELECT * FROM windows
    WHERE :online = 0 OR wm_before_ms IS NULL
       OR wm_before_ms < window_start_ms + 300000 - 1 + 3000000
)
SELECT window_start_ms, window_start_ms + 300000 AS window_end_ms,
       article_id, MAX(title) AS title, MAX(category) AS category, COUNT(*) AS click_count
FROM accepted GROUP BY window_start_ms, article_id
HAVING COUNT(*) > 1000 ORDER BY window_start_ms, article_id;

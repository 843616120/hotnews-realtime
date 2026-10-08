-- 只核验阈值和事件时间边界，六个样本的行为均在首次 Watermark 前到达。
-- 从独立清洗、去重、关联后的原始输入展开五个左闭右开窗口，不读取 Flink 输出。
WITH offsets(n) AS (VALUES(0),(1),(2),(3),(4)), windows AS (
    SELECT j.*, (CAST(event_ms / 60000 AS INTEGER) - n) * 60000 AS window_start_ms
    FROM joined j CROSS JOIN offsets
    WHERE action = 'click'
      AND article_id IN ('count-999', 'count-1000', 'count-1001',
                         'boundary-before', 'boundary-at', 'boundary-after')
)
SELECT window_start_ms, window_start_ms + 300000 AS window_end_ms,
       article_id, MAX(title) AS title, MAX(category) AS category, COUNT(*) AS click_count
FROM windows GROUP BY window_start_ms, article_id
ORDER BY window_start_ms, article_id

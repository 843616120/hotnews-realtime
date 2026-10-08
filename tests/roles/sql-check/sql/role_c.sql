-- 独立基准 C：固定输入的每次点击回看 (event_time - 60 秒, event_time]。
-- 同 IP 不同文章 >50 且平均时长 <2000 毫秒；同一分钟仅取最早告警。
-- 每行的窗口由该次点击时间确定，不能用一分钟滚动窗口替代。
WITH candidates AS (
    SELECT current.event_id, current.ip, current.event_ms AS window_end_ms,
           COUNT(DISTINCT previous.article_id) AS article_count,
           COUNT(*) AS click_count,
           AVG(previous.read_duration_ms) AS avg_read_duration_ms
    FROM joined current JOIN joined previous
      ON previous.ip = current.ip
     AND previous.action = 'click'
     AND previous.event_ms > current.event_ms - 60000
     AND previous.event_ms <= current.event_ms
    WHERE current.action = 'click'
    GROUP BY current.event_id, current.ip, current.event_ms
    HAVING COUNT(DISTINCT previous.article_id) > 50
       AND AVG(previous.read_duration_ms) < 2000
), candidate_articles AS (
    SELECT event_id, json_group_array(article_id) AS article_ids
    FROM (
        SELECT DISTINCT current.event_id, previous.article_id
        FROM joined current JOIN joined previous
          ON previous.ip = current.ip
         AND previous.action = 'click'
         AND previous.event_ms > current.event_ms - 60000
         AND previous.event_ms <= current.event_ms
        WHERE current.action = 'click'
        ORDER BY current.event_id, previous.article_id
    )
    GROUP BY event_id
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY ip, CAST(window_end_ms / 60000 AS INTEGER)
        ORDER BY window_end_ms
    ) AS rank
    FROM candidates
)
SELECT ip, article_count, click_count, avg_read_duration_ms,
       window_end_ms - 60000 AS window_start_ms, window_end_ms,
       candidate_articles.article_ids
FROM ranked JOIN candidate_articles USING (event_id)
WHERE rank = 1
ORDER BY window_end_ms, ip;

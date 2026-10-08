-- Fixed seed=20260927: 97044 unique valid behaviors, A=123, B=60, C=0.
SELECT COUNT(*) AS clean_behaviors FROM clean_behavior;
SELECT COUNT(*) AS article_alert_rows FROM article_alert;
SELECT COUNT(*) AS category_rank_rows FROM category_rank;
SELECT COUNT(*) AS active_ip_alerts FROM ip_alert WHERE retracted = 0;
SELECT window_start_ms, rank_no, category, score, revision, top_articles
FROM category_rank ORDER BY window_start_ms, rank_no LIMIT 5;
SELECT event_id, article_id, event_time, category
FROM clean_behavior ORDER BY event_id LIMIT 5;
SELECT event_type, COUNT(*) AS records FROM pipeline_event GROUP BY event_type;
-- 超过在线保留边界的输入需要单独补算，写入异常表并不等于完成补算。
SELECT event_type,event_id,article_id,dirty_reason,payload
FROM pipeline_event
WHERE event_type IN ('ROLE_A_LATE','ROLE_B_LATE_INPUT','ROLE_B_LATE','ROLE_C_LATE')
ORDER BY event_type,event_id LIMIT 50;
SELECT event_id,article_id,reason
FROM unmatched_behavior
WHERE matched=FALSE;

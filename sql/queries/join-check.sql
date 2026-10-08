SELECT * FROM dirty_data ORDER BY created_at DESC;
SELECT * FROM late_data ORDER BY created_at DESC;
SELECT * FROM unmatched_behavior ORDER BY created_at DESC;

-- Inspect unresolved and compensated behaviors separately.
SELECT * FROM unmatched_behavior WHERE matched = FALSE;
SELECT * FROM unmatched_behavior WHERE matched = TRUE;

-- An article can have several waiting behaviors.
SELECT article_id, matched, COUNT(*) AS behavior_count
FROM unmatched_behavior
GROUP BY article_id, matched;

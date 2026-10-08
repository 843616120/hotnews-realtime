-- Keep the arrival-time exception even after its behavior joins successfully.
-- matched=0: waiting for an article or replay; matched=1: successfully joined.
CREATE TABLE IF NOT EXISTS unmatched_behavior (
    event_id VARCHAR(64) NOT NULL PRIMARY KEY,
    article_id VARCHAR(32) NOT NULL,
    event_time VARCHAR(40) NOT NULL,
    matched BOOLEAN NOT NULL DEFAULT FALSE,
    reason VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    KEY idx_unmatched_article (article_id),
    KEY idx_unmatched_status (matched)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

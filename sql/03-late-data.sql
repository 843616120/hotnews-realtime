-- Records older than the Join watermark minus 65 minutes remain queryable here.
-- Both article and behavior events can be late; the source is part of the key.
CREATE TABLE IF NOT EXISTS late_data (
    source_stream VARCHAR(16) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    article_id VARCHAR(32) NOT NULL,
    event_time VARCHAR(40) NOT NULL,
    watermark_ms BIGINT NOT NULL,
    reason VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (source_stream, event_id),
    KEY idx_late_article (article_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

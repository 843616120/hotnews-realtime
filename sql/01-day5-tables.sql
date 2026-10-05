CREATE TABLE IF NOT EXISTS clean_behavior (
    event_id VARCHAR(40) PRIMARY KEY,
    user_id VARCHAR(32) NOT NULL,
    article_id VARCHAR(32) NOT NULL,
    action VARCHAR(16) NOT NULL,
    ip VARCHAR(45) NOT NULL,
    event_time VARCHAR(40) NOT NULL,
    read_duration_ms INT NOT NULL,
    title VARCHAR(200) NOT NULL,
    category VARCHAR(64) NOT NULL,
    tags JSON NOT NULL
) CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS article_alert (
    window_start_ms BIGINT NOT NULL,
    article_id VARCHAR(32) NOT NULL,
    window_end_ms BIGINT NOT NULL,
    title VARCHAR(200) NOT NULL,
    category VARCHAR(64) NOT NULL,
    click_count BIGINT NOT NULL,
    detect_time VARCHAR(40) NOT NULL,
    PRIMARY KEY(window_start_ms,article_id)
) CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS category_rank (
    window_start_ms BIGINT NOT NULL,
    rank_no TINYINT NOT NULL,
    window_end_ms BIGINT NOT NULL,
    category VARCHAR(64) NOT NULL,
    score BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    top_articles JSON NOT NULL,
    detect_time VARCHAR(40) NOT NULL,
    PRIMARY KEY(window_start_ms,rank_no)
) CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS ip_alert (
    alert_minute_ms BIGINT NOT NULL,
    ip VARCHAR(45) NOT NULL,
    retracted BOOLEAN NOT NULL,
    window_start_ms BIGINT NULL,
    window_end_ms BIGINT NULL,
    article_count INT NULL,
    click_count INT NULL,
    avg_read_duration_ms DOUBLE NULL,
    detect_time VARCHAR(40) NULL,
    PRIMARY KEY(alert_minute_ms,ip)
) CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS pipeline_event (
    event_type VARCHAR(32) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    article_id VARCHAR(32) NULL,
    dirty_reason VARCHAR(64) NULL,
    payload JSON NOT NULL,
    PRIMARY KEY(event_type,event_id)
) CHARACTER SET utf8mb4;

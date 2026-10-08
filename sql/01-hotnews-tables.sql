-- Rule A 的清洗明细：event_id 唯一，重复写入走 UPSERT，不会重复计数。
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

-- Rule A 的热点文章告警：同一文章在同一滑动窗口只有一行。
-- 窗口内迟到点击重新计算后按 (window_start_ms, article_id) 更新原行。
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

-- 独立文章热度状态程序的窗口结果；窗口起点和文章 ID 共同保证幂等更新。
CREATE TABLE IF NOT EXISTS article_heat_state (
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
    revision BIGINT NOT NULL,
    window_start_ms BIGINT NULL,
    window_end_ms BIGINT NULL,
    article_count INT NULL,
    click_count INT NULL,
    avg_read_duration_ms DOUBLE NULL,
    read_duration_sum_ms BIGINT NULL,
    article_ids JSON NULL,
    detect_time VARCHAR(40) NULL,
    PRIMARY KEY(alert_minute_ms,ip)
) CHARACTER SET utf8mb4;

-- 独立 IP 状态程序保存每个已结算窗口，is_alert 表示该窗口是否命中刷量阈值。
CREATE TABLE IF NOT EXISTS ip_window_state (
    window_start_ms BIGINT NOT NULL,
    ip VARCHAR(45) NOT NULL,
    window_end_ms BIGINT NOT NULL,
    article_count INT NOT NULL,
    click_count BIGINT NOT NULL,
    avg_read_duration_ms DOUBLE NOT NULL,
    article_ids JSON NOT NULL,
    is_alert BOOLEAN NOT NULL,
    detect_time VARCHAR(40) NOT NULL,
    PRIMARY KEY(window_start_ms,ip)
) CHARACTER SET utf8mb4;

-- 已有 ip_alert 表的增列见 05-rule-c-ip-alert.sql；兼容 MySQL 8.0.16。

CREATE TABLE IF NOT EXISTS pipeline_event (
    event_type VARCHAR(32) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    article_id VARCHAR(32) NULL,
    dirty_reason VARCHAR(64) NULL,
    payload JSON NOT NULL,
    PRIMARY KEY(event_type,event_id)
) CHARACTER SET utf8mb4;

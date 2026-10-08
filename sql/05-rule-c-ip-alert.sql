-- 在 MySQL 中执行整个文件后启动 Rule C；不删除已有告警数据。
USE hotnews;

-- 同 IP 每个事件分钟保留最早命中的一分钟回看告警，晚到数据按 revision 更新或撤销。
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
    PRIMARY KEY(alert_minute_ms, ip)
) CHARACTER SET utf8mb4;

-- MySQL 8.0.16 不支持 ADD COLUMN IF NOT EXISTS，用元数据判断兼容已有表。
SET @role_c_ddl = IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = 'ip_alert' AND column_name = 'revision') = 0,
    'ALTER TABLE ip_alert ADD COLUMN revision BIGINT NOT NULL DEFAULT 0', 'SELECT 1');
PREPARE role_c_migration FROM @role_c_ddl;
EXECUTE role_c_migration;
DEALLOCATE PREPARE role_c_migration;

SET @role_c_ddl = IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = 'ip_alert' AND column_name = 'article_ids') = 0,
    'ALTER TABLE ip_alert ADD COLUMN article_ids JSON NULL', 'SELECT 1');
PREPARE role_c_migration FROM @role_c_ddl;
EXECUTE role_c_migration;
DEALLOCATE PREPARE role_c_migration;

SET @role_c_ddl = IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = DATABASE() AND table_name = 'ip_alert' AND column_name = 'read_duration_sum_ms') = 0,
    'ALTER TABLE ip_alert ADD COLUMN read_duration_sum_ms BIGINT NULL', 'SELECT 1');
PREPARE role_c_migration FROM @role_c_ddl;
EXECUTE role_c_migration;
DEALLOCATE PREPARE role_c_migration;

-- 查询当前有效告警：
-- SELECT * FROM ip_alert WHERE retracted = FALSE ORDER BY alert_minute_ms, ip;
-- 查询已撤销告警：
-- SELECT * FROM ip_alert WHERE retracted = TRUE;
-- 查询超期行为（pipeline_event 表沿用 01-hotnews-tables.sql）：
-- SELECT * FROM pipeline_event WHERE event_type = 'ROLE_C_LATE';

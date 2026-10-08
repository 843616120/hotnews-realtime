-- 先在 hotnews 执行本文件，再启动新版作业。不删除已有业务数据。
USE hotnews;
SET @ddl = IF((SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema=DATABASE() AND table_name='clean_behavior' AND column_name='event_time_ms')=0,
    'ALTER TABLE clean_behavior ADD COLUMN event_time_ms BIGINT NOT NULL DEFAULT 0', 'SELECT 1');
PREPARE replay_ddl FROM @ddl;
EXECUTE replay_ddl;
DEALLOCATE PREPARE replay_ddl;
SET @ddl = IF((SELECT COUNT(*) FROM information_schema.columns
    WHERE table_schema=DATABASE() AND table_name='clean_behavior' AND column_name='article_version')=0,
    'ALTER TABLE clean_behavior ADD COLUMN article_version INT NOT NULL DEFAULT 1', 'SELECT 1');
PREPARE replay_ddl FROM @ddl;
EXECUTE replay_ddl;
DEALLOCATE PREPARE replay_ddl;
SET @ddl = IF((SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema=DATABASE() AND table_name='clean_behavior' AND index_name='idx_clean_event_time')=0,
    'ALTER TABLE clean_behavior ADD INDEX idx_clean_event_time(event_time_ms)', 'SELECT 1');
PREPARE replay_ddl FROM @ddl;
EXECUTE replay_ddl;
DEALLOCATE PREPARE replay_ddl;
-- 旧记录无法从当前表还原准确文章版本。验证新版时用同一批 Kafka 重新消费，
-- UPSERT 会填入准确 event_time_ms/article_version，不混用其它批次结果。

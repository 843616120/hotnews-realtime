-- Run in MYSQL_DATABASE (default: hotnews). No existing data is deleted.
-- Invalid JSON is stored verbatim as LONGTEXT. record_id is a stable raw-data hash.
CREATE TABLE IF NOT EXISTS dirty_data (
    source_stream VARCHAR(16) NOT NULL,
    record_id VARCHAR(64) NOT NULL,
    reason TEXT NOT NULL,
    raw_data LONGTEXT NOT NULL,
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (source_stream, record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

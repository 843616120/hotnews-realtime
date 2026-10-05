import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

/**
 * MySQL 批量幂等写入。用法：将 DETAIL/A/B/C 的 JSONObject 流分别绑定对应 Kind；
 * 思路：每 200 行或 Checkpoint 前提交一次事务，失败抛出让 Flink 重放；唯一键
 * 使同一行为、同一窗口结果的重复写入覆盖原值而非累加。
 */
public class Day5MySqlSink extends RichSinkFunction<JSONObject> implements CheckpointedFunction {
    /** 明细和三类告警分别映射到独立表，避免混用唯一键。 */
    public enum Kind { DETAIL, ARTICLE_ALERT, CATEGORY_RANK, IP_ALERT, PIPELINE_EVENT }

    private final Kind kind;
    private final Integer configuredBatchSize;
    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient int batchSize;
    private transient int pending;

    public Day5MySqlSink(Kind kind) {
        this(kind, null);
    }

    public Day5MySqlSink(Kind kind, Integer configuredBatchSize) {
        this.kind = kind;
        this.configuredBatchSize = configuredBatchSize;
        if (configuredBatchSize != null) validateBatchSize(configuredBatchSize);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        batchSize = configuredBatchSize == null
                ? Integer.parseInt(System.getenv().getOrDefault("HOTNEWS_MYSQL_BATCH_SIZE", "200"))
                : configuredBatchSize;
        validateBatchSize(batchSize);
        String host = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
        String port = System.getenv().getOrDefault("MYSQL_PORT", "3306");
        String db = System.getenv().getOrDefault("MYSQL_DATABASE", "hotnews");
        String user = System.getenv().getOrDefault("MYSQL_USER", "root");
        String password = System.getenv("MYSQL_PASSWORD");
        if (password == null) {
            throw new IllegalArgumentException("MYSQL_PASSWORD 未配置");
        }
        // Flink 的用户代码类加载器在恢复时可能错过 JDBC SPI 的自动注册。
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(
                "jdbc:mysql://" + host + ":" + port + "/" + db
                        + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
                user, password);
        connection.setAutoCommit(false);
        statement = connection.prepareStatement(sql(kind));
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        bind(statement, kind, value);
        statement.addBatch();
        if (++pending >= batchSize) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // 外部事务在快照前完成；失败后由 Kafka 位点回放，唯一键抵消重复写入。
    }

    @Override
    public void close() throws Exception {
        try {
            flush();
        } finally {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        }
    }

    private void flush() throws SQLException {
        if (pending == 0) return;
        try {
            statement.executeBatch();
            connection.commit();
            pending = 0;
        } catch (SQLException error) {
            connection.rollback();
            throw error;
        }
    }

    static String sql(Kind kind) {
        switch (kind) {
            case DETAIL:
                return "INSERT INTO clean_behavior(event_id,user_id,article_id,action,ip,event_time,"
                        + "read_duration_ms,title,category,tags) VALUES(?,?,?,?,?,?,?,?,?,?) "
                        + "ON DUPLICATE KEY UPDATE user_id=VALUES(user_id),article_id=VALUES(article_id),"
                        + "action=VALUES(action),ip=VALUES(ip),event_time=VALUES(event_time),"
                        + "read_duration_ms=VALUES(read_duration_ms),title=VALUES(title),"
                        + "category=VALUES(category),tags=VALUES(tags)";
            case ARTICLE_ALERT:
                return "INSERT INTO article_alert(window_start_ms,article_id,window_end_ms,title,"
                        + "category,click_count,detect_time) VALUES(?,?,?,?,?,?,?) "
                        + "ON DUPLICATE KEY UPDATE window_end_ms=VALUES(window_end_ms),"
                        + "title=VALUES(title),category=VALUES(category),"
                        + "click_count=GREATEST(click_count,VALUES(click_count)),"
                        + "detect_time=VALUES(detect_time)";
            case CATEGORY_RANK:
                return "INSERT INTO category_rank(window_start_ms,rank_no,window_end_ms,category,"
                        + "score,top_articles,detect_time,revision) VALUES(?,?,?,?,?,?,?,?) "
                        + "ON DUPLICATE KEY UPDATE "
                        + "window_end_ms=IF(VALUES(revision)>=revision,VALUES(window_end_ms),window_end_ms),"
                        + "category=IF(VALUES(revision)>=revision,VALUES(category),category),"
                        + "score=IF(VALUES(revision)>=revision,VALUES(score),score),"
                        + "top_articles=IF(VALUES(revision)>=revision,VALUES(top_articles),top_articles),"
                        + "detect_time=IF(VALUES(revision)>=revision,VALUES(detect_time),detect_time),"
                        + "revision=GREATEST(revision,VALUES(revision))";
            case IP_ALERT:
                return "INSERT INTO ip_alert(alert_minute_ms,ip,retracted,window_start_ms,"
                        + "window_end_ms,article_count,click_count,avg_read_duration_ms,"
                        + "detect_time) VALUES(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE "
                        + "retracted=VALUES(retracted),window_start_ms=VALUES(window_start_ms),"
                        + "window_end_ms=VALUES(window_end_ms),article_count=VALUES(article_count),"
                        + "click_count=VALUES(click_count),"
                        + "avg_read_duration_ms=VALUES(avg_read_duration_ms),"
                        + "detect_time=VALUES(detect_time)";
            case PIPELINE_EVENT:
                return "INSERT INTO pipeline_event(event_type,event_id,article_id,dirty_reason,payload) "
                        + "VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE "
                        + "article_id=VALUES(article_id),dirty_reason=VALUES(dirty_reason),"
                        + "payload=VALUES(payload)";
            default:
                throw new IllegalArgumentException("未知存储类型: " + kind);
        }
    }

    static void bind(PreparedStatement ps, Kind kind, JSONObject value) throws SQLException {
        switch (kind) {
            case DETAIL:
                ps.setString(1, value.getString("event_id"));
                ps.setString(2, value.getString("user_id"));
                ps.setString(3, value.getString("article_id"));
                ps.setString(4, value.getString("action"));
                ps.setString(5, value.getString("ip"));
                ps.setString(6, value.getString("event_time"));
                ps.setInt(7, value.getIntValue("read_duration_ms"));
                ps.setString(8, value.getString("title"));
                ps.setString(9, value.getString("category"));
                ps.setString(10, value.getJSONArray("tags").toJSONString());
                return;
            case ARTICLE_ALERT:
                ps.setLong(1, epoch(value, "window_start"));
                ps.setString(2, value.getString("article_id"));
                ps.setLong(3, epoch(value, "window_end"));
                ps.setString(4, value.getString("title"));
                ps.setString(5, value.getString("category"));
                ps.setLong(6, value.getLongValue("click_count"));
                ps.setString(7, value.getString("detect_time"));
                return;
            case CATEGORY_RANK:
                ps.setLong(1, epoch(value, "window_start"));
                ps.setInt(2, value.getIntValue("rank"));
                ps.setLong(3, epoch(value, "window_end"));
                ps.setString(4, value.getString("category"));
                ps.setLong(5, value.getLongValue("score"));
                ps.setString(6, value.getJSONArray("top_articles").toJSONString());
                ps.setString(7, value.getString("detect_time"));
                ps.setLong(8, value.getLongValue("revision"));
                return;
            case IP_ALERT:
                boolean retracted = value.getBooleanValue("retracted");
                ps.setLong(1, value.getLongValue("alert_minute"));
                ps.setString(2, value.getString("ip"));
                ps.setBoolean(3, retracted);
                if (retracted) {
                    ps.setNull(4, java.sql.Types.BIGINT);
                    ps.setNull(5, java.sql.Types.BIGINT);
                    ps.setNull(6, java.sql.Types.INTEGER);
                    ps.setNull(7, java.sql.Types.INTEGER);
                    ps.setNull(8, java.sql.Types.DOUBLE);
                    ps.setNull(9, java.sql.Types.VARCHAR);
                } else {
                    ps.setLong(4, epoch(value, "window_start"));
                    ps.setLong(5, epoch(value, "window_end"));
                    ps.setInt(6, value.getIntValue("article_count"));
                    ps.setInt(7, value.getIntValue("click_count"));
                    ps.setDouble(8, value.getDoubleValue("avg_read_duration_ms"));
                    ps.setString(9, value.getString("detect_time"));
                }
                return;
            case PIPELINE_EVENT:
                ps.setString(1, value.getString("exception_type"));
                ps.setString(2, value.getString("event_id"));
                ps.setString(3, value.getString("article_id"));
                ps.setString(4, value.getString("dirty_reason"));
                ps.setString(5, value.toJSONString());
                return;
            default:
                throw new IllegalArgumentException("未知存储类型: " + kind);
        }
    }

    private static long epoch(JSONObject value, String key) {
        return OffsetDateTime.parse(value.getString(key)).toInstant().toEpochMilli();
    }

    private static void validateBatchSize(int value) {
        if (value < 1 || value > 10_000) {
            throw new IllegalArgumentException("MySQL batch size 必须为 1..10000");
        }
    }
}

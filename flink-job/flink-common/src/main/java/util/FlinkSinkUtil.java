package util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink 外部 Sink 基础设施。
 *
 * <p>本类只处理所有外部 Sink 都共有的技术问题：连接生命周期、批量发送、
 * JDBC 事务、Checkpoint 前 flush、失败抛出和资源关闭。它不认识业务表、业务
 * 字段、规则类型或 JSON 字段名。具体业务程序通过 Binder/Writer 回调定义 SQL
 * 和字段绑定，这样新增规则时不需要修改公共模块。</p>
 *
 * <p>调用方把返回的 Sink 挂在自己的 DataStream 上，Sink 就是同一个 Flink
 * 拓扑中的算子，不需要再启动第二个消费程序。外部系统与 Flink Checkpoint
 * 不是同一分布式事务，调用方仍必须使用自己的唯一键、版本条件或幂等 Key。</p>
 */
public final class FlinkSinkUtil {
    private FlinkSinkUtil() {
        // 工具类只提供工厂方法和公共 Sink 类型，不保存运行时状态。
    }

    /** 由业务代码把一条记录绑定到 PreparedStatement 的具体字段。 */
    public interface JdbcBinder<T> extends java.io.Serializable {
        void bind(PreparedStatement statement, T value) throws SQLException;
    }

    /** 由业务代码定义单条 Redis 写入，包括 Key、参数和 Lua 脚本。 */
    public interface RedisWriter<T> extends java.io.Serializable {
        void write(Jedis redis, T value) throws Exception;
    }

    /** 把流接入通用 JDBC 批量 Sink，SQL 和字段绑定由调用方提供。 */
    public static <T> DataStreamSink<T> sinkMySql(
            DataStream<T> stream,
            String sql,
            JdbcBinder<T> binder,
            Integer batchSize,
            String name) {
        return stream.addSink(new JdbcBatchSink<T>(sql, binder, batchSize, name)).name(name);
    }

    /** 把流接入由业务 Writer 定义字段和脚本的 Redis 单条 Sink。 */
    public static <T> DataStreamSink<T> sinkRedis(
            DataStream<T> stream,
            RedisWriter<T> writer,
            String name) {
        return stream.addSink(new RedisSink<T>(writer, name)).name(name);
    }

    /**
     * 通用 JDBC 批量 Sink。
     *
     * <p>它不解析 T，也不决定 SQL 参数顺序；invoke 只调用业务方传入的
     * JdbcBinder。达到批次大小或 Checkpoint 到达时提交事务，数据库异常回滚
     * 后继续向 Flink 抛出，使任务按照已完成 Checkpoint 进行恢复。</p>
     */
    public static class JdbcBatchSink<T> extends RichSinkFunction<T>
            implements CheckpointedFunction {
        private final String sql;
        private final JdbcBinder<T> binder;
        private final Integer configuredBatchSize;
        private final String metricName;
        private transient Connection connection;
        private transient PreparedStatement statement;
        private transient int batchSize;
        private transient List<T> pending;
        private transient Counter records;
        private transient Meter rate;

        public JdbcBatchSink(String sql, JdbcBinder<T> binder, Integer configuredBatchSize) {
            this(sql, binder, configuredBatchSize, "mysql");
        }

        public JdbcBatchSink(String sql, JdbcBinder<T> binder, Integer configuredBatchSize,
                             String metricName) {
            if (sql == null || sql.trim().isEmpty()) throw new IllegalArgumentException("SQL 不能为空");
            if (binder == null) throw new IllegalArgumentException("JDBC 字段绑定器不能为空");
            this.sql = sql;
            this.binder = binder;
            this.configuredBatchSize = configuredBatchSize;
            this.metricName = metricKey(metricName, "mysql");
            if (configuredBatchSize != null) validateBatchSize(configuredBatchSize);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            batchSize = configuredBatchSize == null
                    ? Integer.parseInt(System.getenv().getOrDefault("HOTNEWS_MYSQL_BATCH_SIZE", "200"))
                    : configuredBatchSize;
            validateBatchSize(batchSize);
            connection = connectMySql();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);
            pending = new ArrayList<T>(batchSize);
            org.apache.flink.metrics.MetricGroup group = getRuntimeContext()
                    .getMetricGroup().addGroup("throughput", metricName);
            records = group.counter("records");
            rate = group.meter("records_per_second", new MeterView(60));
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            pending.add(value);
            records.inc();
            rate.markEvent();
            if (pending.size() >= batchSize) flush();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 让已进入当前 Sink 的数据先提交，再允许 Checkpoint 完成。
            flush();
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            // 外部事务不复制进 Flink 状态，恢复时由业务幂等键抵消重放。
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

        private void flush() throws Exception {
            if (pending.isEmpty()) return;
            for (int attempt = 1; attempt <= 3; attempt++) {
                try {
                    // 死锁会回滚整个事务；重新绑定这一批数据，不能重试已清空的 JDBC batch。
                    statement.clearBatch();
                    for (T value : pending) {
                        binder.bind(statement, value);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                    connection.commit();
                    pending.clear();
                    return;
                } catch (SQLException error) {
                    connection.rollback();
                    statement.clearBatch();
                    if (!isDeadlock(error) || attempt == 3) throw error;
                    Thread.sleep(100L * attempt);
                }
            }
        }

        private static boolean isDeadlock(SQLException error) {
            for (SQLException current = error; current != null; current = current.getNextException()) {
                if (current.getErrorCode() == 1213 || "40001".equals(current.getSQLState())) return true;
            }
            return false;
        }

        private static void validateBatchSize(int value) {
            if (value < 1 || value > 10_000) {
                throw new IllegalArgumentException("JDBC batch size 必须为 1..10000");
            }
        }
    }

    /**
     * 通用 Redis 单条 Sink。
     *
     * <p>公共类只建立连接和管理生命周期；业务 Writer 自己决定是否使用
     * Lua、Hash、String、Key 及过期时间。</p>
     */
    public static class RedisSink<T> extends RichSinkFunction<T> {
        private final RedisWriter<T> writer;
        private final String metricName;
        private transient Jedis redis;
        private transient Counter records;
        private transient Meter rate;

        public RedisSink(RedisWriter<T> writer) {
            this(writer, "redis");
        }

        public RedisSink(RedisWriter<T> writer, String metricName) {
            if (writer == null) throw new IllegalArgumentException("Redis 写入器不能为空");
            this.writer = writer;
            this.metricName = metricKey(metricName, "redis");
        }

        @Override
        public void open(Configuration parameters) {
            redis = connectRedis();
            redis.ping();
            org.apache.flink.metrics.MetricGroup group = getRuntimeContext()
                    .getMetricGroup().addGroup("throughput", metricName);
            records = group.counter("records");
            rate = group.meter("records_per_second", new MeterView(60));
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            writer.write(redis, value);
            records.inc();
            rate.markEvent();
        }

        @Override
        public void close() {
            if (redis != null) redis.close();
        }
    }

    /** 完整窗口补算与批量 Sink 共用连接配置。 */
    public static Connection connectMySql() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String host = System.getenv().getOrDefault("MYSQL_HOST", "localhost");
        String port = System.getenv().getOrDefault("MYSQL_PORT", "3307");
        String database = System.getenv().getOrDefault("MYSQL_DATABASE", "hotnews");
        return DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database
                        + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
                System.getenv().getOrDefault("MYSQL_USER", "root"),
                System.getenv().getOrDefault("MYSQL_PASSWORD", "root"));
    }

    public static Jedis connectRedis() {
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        Jedis redis = new Jedis(host, port);
        String password = System.getenv("REDIS_PASSWORD");
        if (password != null && !password.isEmpty()) redis.auth(password);
        return redis;
    }

    /** Metric group 名称不能带业务展示文案中的空格，统一转成稳定的安全标识。 */
    private static String metricKey(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) return fallback;
        return value.trim().replaceAll("[^A-Za-z0-9_]+", "_");
    }
}

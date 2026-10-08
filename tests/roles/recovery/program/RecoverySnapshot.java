import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** 只读故障演练快照：不订阅 Kafka、不改位点、不写业务表或 Redis。 */
public class RecoverySnapshot {
    public static void main(String[] args) throws Exception {
        Path directory = Paths.get(args[0]);
        Files.createDirectories(directory);
        JSONObject report = new JSONObject(true);
        report.put("started_at", Instant.now().toString());
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection connection = DriverManager.getConnection(
                "jdbc:mysql://localhost:3307/hotnews?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
                "root", "root")) {
            connection.setReadOnly(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(20);
                JSONObject counts = new JSONObject(true);
                for (String table : Arrays.asList("clean_behavior", "article_alert", "category_rank", "ip_alert",
                        "pipeline_event", "dirty_data", "late_data", "unmatched_behavior")) {
                    try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
                        rs.next(); counts.put(table, rs.getLong(1));
                    }
                }
                report.put("mysql_counts", counts);
                report.put("mysql_active_ip_alerts", rows(statement, "SELECT COUNT(*) AS n FROM ip_alert WHERE retracted=0"));
                report.put("pipeline_events", rows(statement, "SELECT event_type,COUNT(*) AS n FROM pipeline_event GROUP BY event_type ORDER BY event_type"));
                report.put("join_late", rows(statement, "SELECT source_stream,reason,COUNT(*) AS n FROM late_data GROUP BY source_stream,reason ORDER BY source_stream,reason"));
                // 告警较少，保存全部字段以便逐主键比对；不全量导出十万条明细。
                save(directory.resolve("article_alert.json"), rows(statement, "SELECT * FROM article_alert ORDER BY window_start_ms,article_id"));
                save(directory.resolve("category_rank.json"), rows(statement, "SELECT * FROM category_rank ORDER BY window_start_ms,rank_no"));
                save(directory.resolve("ip_alert.json"), rows(statement, "SELECT * FROM ip_alert ORDER BY alert_minute_ms,ip"));
            } finally { connection.rollback(); }
        } catch (Exception error) { report.put("mysql_error", error.toString()); }
        try (Jedis redis = new Jedis("localhost", 6379)) {
            JSONObject value = new JSONObject(true);
            value.put("key", "hotnews:top5:latest");
            value.put("hash", redis.hgetAll("hotnews:top5:latest"));
            value.put("ttl_seconds", redis.ttl("hotnews:top5:latest"));
            report.put("redis", value);
        } catch (Exception error) { report.put("redis_error", error.toString()); }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("request.timeout.ms", "10000");
        properties.put("default.api.timeout.ms", "15000");
        try (AdminClient admin = AdminClient.create(properties)) {
            JSONArray groups = new JSONArray();
            for (ConsumerGroupListing group : admin.listConsumerGroups().all().get(15, TimeUnit.SECONDS)) {
                if (!group.groupId().startsWith("hotnews-")) continue;
                Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group.groupId())
                        .partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
                Map<TopicPartition, OffsetSpec> request = new HashMap<>();
                for (TopicPartition partition : committed.keySet()) request.put(partition, OffsetSpec.latest());
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(request)
                        .all().get(15, TimeUnit.SECONDS);
                JSONObject item = new JSONObject(true);
                item.put("group", group.groupId());
                JSONArray partitions = new JSONArray();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : committed.entrySet()) {
                    JSONObject partition = new JSONObject(true);
                    long end = ends.get(offset.getKey()).offset();
                    partition.put("topic", offset.getKey().topic());
                    partition.put("partition", offset.getKey().partition());
                    partition.put("committed", offset.getValue().offset());
                    partition.put("end", end);
                    partition.put("committed_lag", end - offset.getValue().offset());
                    partitions.add(partition);
                }
                item.put("partitions", partitions);
                groups.add(item);
            }
            report.put("kafka_groups", groups);
        } catch (Exception error) { report.put("kafka_error", error.toString()); }
        report.put("completed_at", Instant.now().toString());
        save(directory.resolve("external-snapshot.json"), report);
        System.out.println(report.toJSONString());
    }

    private static JSONArray rows(Statement statement, String sql) throws SQLException {
        JSONArray rows = new JSONArray();
        try (ResultSet rs = statement.executeQuery(sql)) {
            ResultSetMetaData metadata = rs.getMetaData();
            while (rs.next()) {
                JSONObject row = new JSONObject(true);
                for (int i = 1; i <= metadata.getColumnCount(); i++) row.put(metadata.getColumnLabel(i), rs.getObject(i));
                rows.add(row);
            }
        }
        return rows;
    }

    private static void save(Path path, Object value) throws Exception {
        Files.write(path, JSONObject.toJSONString(value, true).getBytes(StandardCharsets.UTF_8));
    }
}

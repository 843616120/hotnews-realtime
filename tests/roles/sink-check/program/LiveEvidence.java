import com.agd.flink.roles.rolea.Achieve_roleA;
import com.agd.flink.roles.roleb.Achieve_roleB;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import model.CategoryRankingSnapshot;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import redis.clients.jedis.Jedis;
import util.RoleStreamUtil;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** 现场只读快照，以及只操作临时表/专属测试 Key 的 Sink 契约验收。 */
public class LiveEvidence {
    public static void main(String[] args) throws Exception {
        Path directory = Paths.get(args[1]);
        if ("snapshot".equals(args[0])) snapshot(directory);
        else if ("sink-check".equals(args[0])) sinkCheck(directory, args[2]);
        else throw new IllegalArgumentException("snapshot|sink-check");
    }

    private static Connection mysql() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://localhost:3307/hotnews"
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root");
    }

    private static void snapshot(Path directory) throws Exception {
        JSONObject report = new JSONObject();
        report.put("captured_at", Instant.now().toString());
        try (Connection connection = mysql(); Statement statement = connection.createStatement()) {
            connection.setReadOnly(true);
            connection.setAutoCommit(false);
            JSONObject tables = new JSONObject();
            for (String table : Arrays.asList("clean_behavior", "article_alert", "category_rank", "ip_alert",
                    "pipeline_event", "dirty_data", "late_data", "unmatched_behavior")) {
                JSONObject evidence = new JSONObject();
                evidence.put("schema", rows(statement.executeQuery("SHOW CREATE TABLE " + table)));
                JSONArray data = rows(statement.executeQuery("SELECT * FROM " + table));
                save(directory.resolve(table + ".json"), data.toJSONString());
                evidence.put("rows", data.size());
                tables.put(table, evidence);
            }
            report.put("mysql", tables);
            // 同一一致性读事务中的数据快照，不更改业务表。
            connection.rollback();
        } catch (Exception error) {
            report.put("mysql_error", error.toString());
        }
        try (Jedis redis = new Jedis("localhost", 6379)) {
            JSONObject state = new JSONObject();
            state.put("key", "hotnews:top5:latest");
            state.put("type", redis.type("hotnews:top5:latest"));
            state.put("value", redis.hgetAll("hotnews:top5:latest"));
            state.put("ttl_seconds", redis.ttl("hotnews:top5:latest"));
            report.put("redis", state);
        } catch (Exception error) {
            report.put("redis_error", error.toString());
        }
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("request.timeout.ms", "10000");
        properties.setProperty("default.api.timeout.ms", "15000");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (AdminClient admin = AdminClient.create(properties);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            JSONObject groups = new JSONObject();
            for (ConsumerGroupListing group : admin.listConsumerGroups().all().get(15, TimeUnit.SECONDS)) {
                if (!group.groupId().startsWith("hotnews-role-")) continue;
                JSONArray positions = new JSONArray();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> offset : admin.listConsumerGroupOffsets(
                        group.groupId()).partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS).entrySet()) {
                    JSONObject row = new JSONObject();
                    row.put("topic", offset.getKey().topic());
                    row.put("partition", offset.getKey().partition());
                    row.put("committed", offset.getValue().offset());
                    positions.add(row);
                }
                JSONObject groupInfo = new JSONObject();
                groupInfo.put("offsets", positions);
                groupInfo.put("description", admin.describeConsumerGroups(Arrays.asList(group.groupId()))
                        .all().get(15, TimeUnit.SECONDS).get(group.groupId()).toString());
                groups.put(group.groupId(), groupInfo);
            }
            report.put("consumer_groups", groups);
            JSONObject topics = new JSONObject();
            for (String topic : Arrays.asList("topic_article", "topic_behavior")) {
                List<TopicPartition> partitions = new ArrayList<>();
                consumer.partitionsFor(topic).forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
                Map<TopicPartition, Long> starts = consumer.beginningOffsets(partitions);
                Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
                consumer.assign(partitions);
                for (TopicPartition partition : partitions) consumer.seek(partition, starts.get(partition));
                List<String> records = new ArrayList<>();
                long expected = ends.values().stream().mapToLong(Long::longValue).sum()
                        - starts.values().stream().mapToLong(Long::longValue).sum();
                if (expected > 500000) throw new IllegalStateException("只读取证上限 500000，当前 " + expected);
                long deadline = System.currentTimeMillis() + 60000;
                while (System.currentTimeMillis() < deadline) {
                    boolean complete = true;
                    for (TopicPartition partition : partitions) {
                        if (consumer.position(partition) < ends.get(partition)) complete = false;
                    }
                    if (complete) break;
                    ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<String, String> record : polled) {
                        TopicPartition partition = new TopicPartition(topic, record.partition());
                        if (record.offset() >= ends.get(partition)) continue;
                        JSONObject row = new JSONObject();
                        row.put("partition", record.partition()); row.put("offset", record.offset());
                        row.put("timestamp", record.timestamp()); row.put("value", record.value());
                        records.add(row.toJSONString());
                    }
                }
                Files.write(directory.resolve(topic + ".jsonl"), records, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW);
                JSONArray ranges = new JSONArray();
                boolean complete = true;
                for (TopicPartition partition : partitions) {
                    JSONObject range = new JSONObject();
                    range.put("partition", partition.partition());
                    range.put("begin", starts.get(partition)); range.put("end", ends.get(partition));
                    range.put("read_position", consumer.position(partition));
                    if (consumer.position(partition) < ends.get(partition)) complete = false;
                    ranges.add(range);
                }
                JSONObject result = new JSONObject();
                result.put("ranges", ranges); result.put("records", records.size());
                result.put("complete", complete);
                topics.put(topic, result);
            }
            report.put("kafka", topics);
        } catch (Exception error) {
            report.put("kafka_error", error.toString());
        }
        save(directory.resolve("live-snapshot.json"), report.toJSONString());
        System.out.println(report.toJSONString());
    }

    private static void sinkCheck(Path directory, String runId) throws Exception {
        JSONArray checks = new JSONArray();
        try (Connection connection = mysql(); Statement statement = connection.createStatement()) {
            String businessA = rows(statement.executeQuery(
                    "SELECT * FROM article_alert ORDER BY window_start_ms,article_id")).toJSONString();
            String businessB = rows(statement.executeQuery(
                    "SELECT * FROM category_rank ORDER BY window_start_ms,rank_no")).toJSONString();
            // 临时表仅属于本连接，自动消失；业务表不会收到任何测试写入。
            for (String table : Arrays.asList("clean_behavior", "article_alert", "category_rank")) {
                statement.execute("CREATE TEMPORARY TABLE ab_accept_" + table + " LIKE " + table);
            }
            connection.setAutoCommit(false);
            List<JSONObject> a = results(directory.resolve("role-a-output.jsonl"));
            List<JSONObject> b = results(directory.resolve("role-b-output.jsonl"));
            batch(connection, Achieve_roleA.MYSQL_SQL.replace("article_alert", "ab_accept_article_alert"),
                    a, Achieve_roleA::bindMySql);
            long aCount = scalar(statement, "SELECT COUNT(*) FROM ab_accept_article_alert");
            batch(connection, Achieve_roleA.MYSQL_SQL.replace("article_alert", "ab_accept_article_alert"),
                    a, Achieve_roleA::bindMySql);
            check(checks, "A batch replay", aCount, scalar(statement, "SELECT COUNT(*) FROM ab_accept_article_alert"));
            JSONObject largest = a.stream().max(java.util.Comparator.comparingLong(
                    value -> value.getLongValue("click_count"))).get();
            JSONObject lower = JSON.parseObject(largest.toJSONString());
            lower.put("click_count", 1001);
            long start = Instant.parse(lower.getString("window_start")).toEpochMilli();
            String key = " WHERE window_start_ms=" + start + " AND article_id='" + lower.getString("article_id") + "'";
            long before = scalar(statement, "SELECT click_count FROM ab_accept_article_alert" + key);
            check(checks, "A GREATEST test has a genuinely smaller replay", true, before > 1001);
            batch(connection, Achieve_roleA.MYSQL_SQL.replace("article_alert", "ab_accept_article_alert"),
                    Arrays.asList(lower), Achieve_roleA::bindMySql);
            check(checks, "A GREATEST rejects smaller history", before,
                    scalar(statement, "SELECT click_count FROM ab_accept_article_alert" + key));
            batch(connection, Achieve_roleB.MYSQL_SQL.replace("category_rank", "ab_accept_category_rank"),
                    b, Achieve_roleB::bindMySql);
            JSONArray bBefore = rows(statement.executeQuery("SELECT * FROM ab_accept_category_rank ORDER BY window_start_ms,rank_no"));
            List<JSONObject> reversed = new ArrayList<>(b);
            java.util.Collections.reverse(reversed);
            batch(connection, Achieve_roleB.MYSQL_SQL.replace("category_rank", "ab_accept_category_rank"),
                    reversed, Achieve_roleB::bindMySql);
            check(checks, "B older revisions and replay preserve rows", bBefore.toJSONString(),
                    rows(statement.executeQuery("SELECT * FROM ab_accept_category_rank ORDER BY window_start_ms,rank_no")).toJSONString());
            JSONObject clean = new JSONObject();
            clean.put("event_id", "ab-clean-duplicate"); clean.put("user_id", "acceptance");
            clean.put("article_id", "acceptance"); clean.put("action", "click"); clean.put("ip", "127.0.0.1");
            clean.put("event_time", "2026-09-27T00:00:00.001Z"); clean.put("read_duration_ms", 1);
            clean.put("title", "验收"); clean.put("category", "acceptance"); clean.put("tags", new JSONArray());
            batch(connection, RoleStreamUtil.MYSQL_SQL.replace("clean_behavior", "ab_accept_clean_behavior"),
                    Arrays.asList(clean, clean), RoleStreamUtil::bindMySql);
            check(checks, "clean event_id unique replay", 1L, scalar(statement, "SELECT COUNT(*) FROM ab_accept_clean_behavior"));
            JSONObject temporaryResults = new JSONObject();
            temporaryResults.put("a", rows(statement.executeQuery(
                    "SELECT * FROM ab_accept_article_alert ORDER BY window_start_ms,article_id")));
            temporaryResults.put("b", rows(statement.executeQuery(
                    "SELECT * FROM ab_accept_category_rank ORDER BY window_start_ms,rank_no")));
            temporaryResults.put("clean", rows(statement.executeQuery("SELECT * FROM ab_accept_clean_behavior")));
            save(directory.resolve("mysql-test-results.json"), temporaryResults.toJSONString());
            check(checks, "Production article_alert unchanged", businessA,
                    rows(statement.executeQuery("SELECT * FROM article_alert ORDER BY window_start_ms,article_id")).toJSONString());
            check(checks, "Production category_rank unchanged", businessB,
                    rows(statement.executeQuery("SELECT * FROM category_rank ORDER BY window_start_ms,rank_no")).toJSONString());
        }
        try (Jedis redis = new Jedis("localhost", 6379)) {
            Map<String, String> businessRanking = redis.hgetAll("hotnews:top5:latest");
            String key = "hotnews:acceptance:ab:" + runId;
            if (redis.exists(key)) throw new IllegalStateException("测试 Key 已存在，不覆盖：" + key);
            List<JSONObject> b = results(directory.resolve("role-b-output.jsonl"));
            JSONObject first = b.stream().filter(v -> v.getIntValue("rank") == 1 && v.containsKey("ranking")).findFirst().get();
            CategoryRankingSnapshot original = Achieve_roleB.toRedisSnapshot(first);
            CategoryRankingSnapshot initial = new CategoryRankingSnapshot(key, original.windowStartMs,
                    original.windowEnd, original.revision, original.rankingJson, original.expireSeconds);
            Achieve_roleB.writeRedisRanking(redis, initial);
            check(checks, "Redis Hash fields", 4L, redis.hlen(key));
            check(checks, "Redis initial ranking exact", initial.rankingJson, redis.hget(key, "ranking"));
            check(checks, "Redis TTL assigned", 7200L, redis.ttl(key));
            Thread.sleep(1200);
            long countdown = redis.ttl(key);
            check(checks, "Redis TTL decreases", true, countdown > 0 && countdown < 7200);
            JSONObject lastSameWindow = b.stream().filter(v -> v.getIntValue("rank") == 1
                    && v.getString("window_start").equals(first.getString("window_start")))
                    .max(java.util.Comparator.comparingLong(v -> v.getLongValue("revision"))).get();
            CategoryRankingSnapshot actualRevision = Achieve_roleB.toRedisSnapshot(lastSameWindow);
            CategoryRankingSnapshot revised = new CategoryRankingSnapshot(key, actualRevision.windowStartMs,
                    actualRevision.windowEnd, actualRevision.revision, actualRevision.rankingJson, 7200);
            Achieve_roleB.writeRedisRanking(redis, revised);
            check(checks, "Redis same-window revision", Long.toString(revised.revision), redis.hget(key, "revision"));
            check(checks, "Redis revision updates full ranking", revised.rankingJson, redis.hget(key, "ranking"));
            check(checks, "Redis accepted update refreshes TTL", 7200L, redis.ttl(key));
            Achieve_roleB.writeRedisRanking(redis, initial);
            check(checks, "Redis lower revision rejected", Long.toString(revised.revision), redis.hget(key, "revision"));
            JSONObject nextWindow = b.stream().filter(v -> v.getIntValue("rank") == 1
                    && !v.getString("window_start").equals(first.getString("window_start"))).findFirst().get();
            CategoryRankingSnapshot actualNext = Achieve_roleB.toRedisSnapshot(nextWindow);
            CategoryRankingSnapshot newer = new CategoryRankingSnapshot(key, actualNext.windowStartMs,
                    actualNext.windowEnd, actualNext.revision, actualNext.rankingJson, 7200);
            Achieve_roleB.writeRedisRanking(redis, newer);
            check(checks, "Redis newer window replaces complete ranking", newer.rankingJson, redis.hget(key, "ranking"));
            Thread.sleep(1200);
            Map<String, String> before = redis.hgetAll(key);
            long ttlBefore = redis.ttl(key);
            Achieve_roleB.writeRedisRanking(redis, revised);
            check(checks, "Redis old window cannot overwrite latest", before, redis.hgetAll(key));
            check(checks, "Redis rejected old window does not refresh TTL", true, redis.ttl(key) <= ttlBefore);
            JSONObject testKey = new JSONObject();
            testKey.put("key", key); testKey.put("value", redis.hgetAll(key));
            testKey.put("ttl_seconds", redis.ttl(key)); testKey.put("natural_expiry", "未验证：未等待两小时");
            save(directory.resolve("redis-test-key.json"), testKey.toJSONString());
            check(checks, "Production Redis latest ranking unchanged", businessRanking,
                    redis.hgetAll("hotnews:top5:latest"));
            // 不删除 Key；由规则原有的 7200 秒 TTL 自然过期。
        }
        save(directory.resolve("sink-checks.json"), checks.toJSONString());
        System.out.println(checks.toJSONString());
        if (checks.stream().anyMatch(value -> !((JSONObject) value).getBooleanValue("passed"))) {
            throw new IllegalStateException("Sink 验收有失败，见 sink-checks.json");
        }
    }

    private static List<JSONObject> results(Path path) throws Exception {
        List<JSONObject> result = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            JSONObject row = JSON.parseObject(line);
            if ("result".equals(row.getString("kind"))) result.add(row.getJSONObject("value"));
        }
        return result;
    }
    private static void batch(Connection connection, String sql, List<JSONObject> values,
                              util.FlinkSinkUtil.JdbcBinder<JSONObject> binder) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (JSONObject value : values) { binder.bind(statement, value); statement.addBatch(); }
            statement.executeBatch(); connection.commit();
        }
    }
    private static long scalar(Statement statement, String sql) throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) { result.next(); return result.getLong(1); }
    }
    private static JSONArray rows(ResultSet result) throws Exception {
        try (ResultSet data = result) {
            JSONArray rows = new JSONArray();
            ResultSetMetaData columns = data.getMetaData();
            while (data.next()) {
                JSONObject row = new JSONObject();
                for (int i = 1; i <= columns.getColumnCount(); i++) row.put(columns.getColumnLabel(i), data.getObject(i));
                rows.add(row);
            }
            return rows;
        }
    }
    private static void check(JSONArray checks, String name, Object expected, Object actual) {
        JSONObject check = new JSONObject();
        check.put("name", name); check.put("expected", expected); check.put("actual", actual);
        check.put("passed", expected.equals(actual));
        checks.add(check);
    }
    private static void save(Path path, String text) throws Exception {
        Files.write(path, text.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);
    }
}

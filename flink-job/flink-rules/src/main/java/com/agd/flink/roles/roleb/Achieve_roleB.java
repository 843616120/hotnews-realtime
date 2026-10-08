package com.agd.flink.roles.roleb;

import com.agd.flink.join.ArticleJoinBehavior;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import model.CategoryRankingSnapshot;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import redis.clients.jedis.Jedis;
import util.FlinkMetricsUtil;
import util.FlinkRuntimeUtil;
import util.FlinkSinkUtil;
import util.RoleStreamUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 规则 B：10 分钟滚动窗口统计 click、share、comment，输出分类 Top 5 和各类 top_articles[]。 */
public class Achieve_roleB {
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("role-b-late") {};

    public static OutputTag<JSONObject> lateTag() {
        return lateTag;
    }

    public static void main(String[] args) throws Exception {
        // IDEA 本地 Web UI 使用 8084，与 Docker 的 8081 和规则 A 的 8083 错开。
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 8084);
        configuration.set(RestOptions.BIND_PORT, "8084");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(3);
        FlinkRuntimeUtil.configureCheckpointing(env, "HOTNEWS_CHECKPOINT_DIR");

        // 与规则 A 相同：独立消费两条 Kafka 流，Join 后保留行为事件时间和上游 Watermark。
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env, "hotnews-role-b");
        SingleOutputStreamOperator<JSONObject> clean = RoleStreamUtil.prepare(joined);
        FlinkSinkUtil.sinkMySql(clean, RoleStreamUtil.MYSQL_SQL,
                RoleStreamUtil::bindMySql, null, "MySQL clean behavior");

        SingleOutputStreamOperator<JSONObject> result = buildRule(clean);
        DataStream<JSONObject> lateEvents = result.getSideOutput(lateTag)
                .map(value -> RoleStreamUtil.audit(value, "ROLE_B_LATE"));
        FlinkSinkUtil.sinkMySql(lateEvents, RoleStreamUtil.AUDIT_MYSQL_SQL,
                RoleStreamUtil::bindAudit, null, "MySQL role B audit events");

        // 每个窗口输出最多五行供 SQL 核验；第一行还携带完整榜单，写入 Redis 最新榜单。
        // 窗口的迟到修正会反复更新同一组窗口+名次主键。指标和 Sink 都使用单并行度，
        // 避免 REBALANCE 后三个 JDBC 事务交叉锁住同一批排名记录。
        DataStream<JSONObject> measuredResult = FlinkMetricsUtil.measure(result, "role_b_result")
                .setParallelism(1);
        FlinkSinkUtil.sinkMySql(measuredResult, MYSQL_SQL, Achieve_roleB::bindMySql,
                null, "MySQL category ranks").setParallelism(1);
        DataStream<CategoryRankingSnapshot> snapshots = result
                .filter(value -> value.getIntValue("rank") == 1)
                .map(Achieve_roleB::toRedisSnapshot);
        FlinkSinkUtil.sinkRedis(FlinkMetricsUtil.measure(snapshots, "role_b_redis_snapshots"),
                Achieve_roleB::writeRedisRanking, "Redis latest category ranking").setParallelism(1);
        result.print("ROLE_B");
        result.getSideOutput(lateTag).print("ROLE_B_LATE");
        env.execute("Achieve_roleB");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        // 输入已由 Join 校验 action 并去重；一条行为给所属文章和分类都加 1。
        // 500 篇文章规模可在一个窗口内直接汇总，避免两级排名状态和额外定时器。
        // windowAll 本身使用单并行度；下游新建的 Map 和 Sink 需单独配置并行度。
        return joined.windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
                .allowedLateness(Time.minutes(65))
                .sideOutputLateData(lateTag)
                .aggregate(new CountActions(), new EmitTopFive())
                .setParallelism(1);
    }

    public static class ArticleScore {
        public long windowStart;
        public String articleId;
        public String category;
        public String title;
        public long count;
        public int version;
        public long latestTime;
    }

    public static class RankingAccumulator {
        public Map<String, Map<String, ArticleScore>> categories = new HashMap<>();
    }

    private static class CountActions implements AggregateFunction<JSONObject, RankingAccumulator, RankingAccumulator> {
        @Override
        public RankingAccumulator createAccumulator() {
            return new RankingAccumulator();
        }

        @Override
        public RankingAccumulator add(JSONObject value, RankingAccumulator accumulator) {
            String category = value.getString("category");
            String articleId = value.getString("article_id");
            Map<String, ArticleScore> articles = accumulator.categories
                    .computeIfAbsent(category, ignored -> new HashMap<>());
            ArticleScore score = articles.computeIfAbsent(articleId, ignored -> new ArticleScore());
            score.articleId = articleId;
            score.category = category;
            score.count++;
            long eventTime = RoleStreamUtil.eventTime(value);
            int version = value.getIntValue("article_version");
            if (score.count == 1 || version > score.version
                    || (version == score.version && eventTime >= score.latestTime)) {
                score.version = version;
                score.latestTime = eventTime;
                score.title = value.getString("title");
            }
            return accumulator;
        }

        @Override
        public RankingAccumulator getResult(RankingAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public RankingAccumulator merge(RankingAccumulator left, RankingAccumulator right) {
            for (Map.Entry<String, Map<String, ArticleScore>> category : right.categories.entrySet()) {
                Map<String, ArticleScore> target = left.categories
                        .computeIfAbsent(category.getKey(), ignored -> new HashMap<>());
                for (ArticleScore incoming : category.getValue().values()) {
                    ArticleScore existing = target.get(incoming.articleId);
                    if (existing == null) {
                        target.put(incoming.articleId, incoming);
                    } else {
                        existing.count += incoming.count;
                        if (incoming.version > existing.version || (incoming.version == existing.version
                                && incoming.latestTime >= existing.latestTime)) {
                            existing.version = incoming.version;
                            existing.latestTime = incoming.latestTime;
                            existing.title = incoming.title;
                        }
                    }
                }
            }
            return left;
        }
    }

    private static class EmitTopFive extends ProcessAllWindowFunction<RankingAccumulator, JSONObject, TimeWindow> {
        @Override
        public void process(Context context, Iterable<RankingAccumulator> values, Collector<JSONObject> out) {
            Map<String, Map<String, ArticleScore>> grouped = values.iterator().next().categories;
            Map<String, Long> totals = new HashMap<>();
            long revision = 0;
            for (Map.Entry<String, Map<String, ArticleScore>> group : grouped.entrySet()) {
                long total = 0;
                for (ArticleScore article : group.getValue().values()) total += article.count;
                totals.put(group.getKey(), total);
                revision += total;
            }
            List<String> categories = new ArrayList<>(totals.keySet());
            categories.sort(Comparator.<String>comparingLong(name -> -totals.get(name)).thenComparing(name -> name));
            List<JSONObject> rows = new ArrayList<>();
            JSONArray ranking = new JSONArray();
            for (int index = 0; index < Math.min(5, categories.size()); index++) {
                String category = categories.get(index);
                List<ArticleScore> articles = new ArrayList<>(grouped.get(category).values());
                articles.sort(Comparator.<ArticleScore>comparingLong(article -> -article.count)
                        .thenComparing(article -> article.articleId));
                JSONArray topArticles = new JSONArray();
                for (int i = 0; i < Math.min(5, articles.size()); i++) {
                    ArticleScore article = articles.get(i);
                    JSONObject item = new JSONObject();
                    item.put("article_id", article.articleId);
                    item.put("title", article.title);
                    item.put("score", article.count);
                    topArticles.add(item);
                }
                JSONObject row = new JSONObject();
                row.put("rank", index + 1);
                row.put("category", category);
                row.put("score", totals.get(category));
                row.put("top_articles", topArticles);
                row.put("window_start", Instant.ofEpochMilli(context.window().getStart()).toString());
                row.put("window_end", Instant.ofEpochMilli(context.window().getEnd()).toString());
                row.put("detect_time", Instant.now().toString());
                row.put("revision", revision);
                rows.add(row);
                JSONObject snapshotRow = new JSONObject();
                snapshotRow.putAll(row);
                ranking.add(snapshotRow);
            }
            if (!rows.isEmpty()) rows.get(0).put("ranking", ranking);
            for (JSONObject row : rows) out.collect(row);
        }
    }

    public static final String MYSQL_SQL =
            "INSERT INTO category_rank(window_start_ms,rank_no,window_end_ms,category,"
                    + "score,top_articles,detect_time,revision) VALUES(?,?,?,?,?,?,?,?) "
                    + "ON DUPLICATE KEY UPDATE "
                    + "window_end_ms=IF(VALUES(revision)>=revision,VALUES(window_end_ms),window_end_ms),"
                    + "category=IF(VALUES(revision)>=revision,VALUES(category),category),"
                    + "score=IF(VALUES(revision)>=revision,VALUES(score),score),"
                    + "top_articles=IF(VALUES(revision)>=revision,VALUES(top_articles),top_articles),"
                    + "detect_time=IF(VALUES(revision)>=revision,VALUES(detect_time),detect_time),"
                    + "revision=GREATEST(revision,VALUES(revision))";

    public static void bindMySql(PreparedStatement statement, JSONObject value) throws SQLException {
        statement.setLong(1, epoch(value, "window_start"));
        statement.setInt(2, value.getIntValue("rank"));
        statement.setLong(3, epoch(value, "window_end"));
        statement.setString(4, value.getString("category"));
        statement.setLong(5, value.getLongValue("score"));
        statement.setString(6, value.getJSONArray("top_articles").toJSONString());
        statement.setString(7, value.getString("detect_time"));
        statement.setLong(8, value.getLongValue("revision"));
    }

    private static long epoch(JSONObject value, String key) {
        return OffsetDateTime.parse(value.getString(key)).toInstant().toEpochMilli();
    }

    // Redis Hash: 固定 Key 保留最新窗口；相同窗口只接受更大 revision 的迟到修正。
    public static final String REDIS_SCRIPT =
            "local previous=tonumber(redis.call('HGET',KEYS[1],'window_start_ms') or '-1') "
                    + "local incoming=tonumber(ARGV[1]) "
                    + "if incoming < previous then return 0 end "
                    + "local previousRevision=tonumber(redis.call('HGET',KEYS[1],'revision') or '-1') "
                    + "local revision=tonumber(ARGV[4]) "
                    + "if incoming == previous and revision < previousRevision then return 0 end "
                    + "if incoming > previous then redis.call('DEL',KEYS[1]) end "
                    + "redis.call('HSET',KEYS[1],'window_start_ms',ARGV[1],"
                    + "'window_end',ARGV[2],'ranking',ARGV[3],'revision',ARGV[4]) "
                    + "redis.call('EXPIRE',KEYS[1],ARGV[5]) return 1";

    /** Key=hotnews:top5:latest，Value 为 Hash；无新榜单满两小时自动过期。 */
    public static CategoryRankingSnapshot toRedisSnapshot(JSONObject value) {
        return new CategoryRankingSnapshot("hotnews:top5:latest", epoch(value, "window_start"),
                value.getString("window_end"), value.getLongValue("revision"),
                value.getJSONArray("ranking").toJSONString(), 7200);
    }

    public static void writeRedisRanking(Jedis redis, CategoryRankingSnapshot snapshot) {
        redis.eval(REDIS_SCRIPT, Collections.singletonList(snapshot.redisKey), Arrays.asList(
                Long.toString(snapshot.windowStartMs), snapshot.windowEnd,
                snapshot.rankingJson, Long.toString(snapshot.revision),
                Integer.toString(snapshot.expireSeconds)));
    }

    /** 供已有的审计调用者将文章得分转换为稳定的超期记录。 */
    public static JSONObject lateRankScore(ArticleScore score) {
        JSONObject result = new JSONObject();
        result.put("exception_type", "ROLE_B_LATE");
        result.put("event_id", score.articleId + "-" + score.windowStart);
        result.put("article_id", score.articleId);
        result.put("category", score.category);
        result.put("window_start_ms", score.windowStart);
        result.put("score", score.count);
        result.put("dirty_reason", "RANK_STATE_EXPIRED");
        return result;
    }
}

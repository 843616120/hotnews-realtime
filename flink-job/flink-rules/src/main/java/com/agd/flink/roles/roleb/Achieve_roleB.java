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
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import util.CoalescingEventTimeTrigger;

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
    public static final int AGGREGATION_PARALLELISM = 3;
    public static final int RANK_PARALLELISM = 2;
    private static final int SALTS = 8;
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
        // 相同窗口+名次只交给一个 JDBC 子任务，避免不同事务争用相同主键。
        DataStream<JSONObject> measuredResult = FlinkMetricsUtil.measure(result, "role_b_result")
                .setParallelism(RANK_PARALLELISM);
        sinkRanks(measuredResult);
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
        // 第一阶段按分类+盐分片：同一热点文章的行为可在多个子任务计数。
        // 保留分片内所有文章，不能提前截断局部 Top5，否则全局排名可能漏文章。
        SingleOutputStreamOperator<ShardSnapshot> shards = joined
                .keyBy(value -> value.getString("category") + "#"
                        + Math.floorMod(value.getString("event_id").hashCode(), SALTS))
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .allowedLateness(Time.minutes(65))
                .sideOutputLateData(lateTag)
                .trigger(new CoalescingEventTimeTrigger())
                .aggregate(new CountActions(), new EmitShard())
                .name("B category salted windows").setParallelism(AGGREGATION_PARALLELISM);
        SingleOutputStreamOperator<JSONObject> ranked = shards.keyBy(value -> value.windowStart)
                .process(new MergeRanking()).name("B merge window rankings")
                .setParallelism(RANK_PARALLELISM);
        // 迟到旁路属于第一阶段，显式转接，保留调用方原来的旁路查询入口。
        return ranked.connect(shards.getSideOutput(lateTag))
                .process(new org.apache.flink.streaming.api.functions.co.CoProcessFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement1(JSONObject value, Context ctx, Collector<JSONObject> out) {
                        out.collect(value);
                    }
                    @Override
                    public void processElement2(JSONObject value, Context ctx, Collector<JSONObject> out) {
                        ctx.output(lateTag, value);
                    }
                }).name("B results and late audit").setParallelism(RANK_PARALLELISM);
    }

    public static void sinkRanks(DataStream<JSONObject> result) {
        FlinkSinkUtil.sinkMySql(result.keyBy(value -> value.getString("window_start")
                        + "#" + value.getIntValue("rank")), MYSQL_SQL, Achieve_roleB::bindMySql,
                null, "MySQL category ranks").setParallelism(RANK_PARALLELISM).disableChaining();
    }

    public static class ShardSnapshot {
        public String shard;
        public long windowStart;
        public long count;
        public RankingAccumulator value;
    }

    private static class EmitShard extends ProcessWindowFunction<RankingAccumulator, ShardSnapshot, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<RankingAccumulator> values,
                            Collector<ShardSnapshot> out) {
            ShardSnapshot snapshot = new ShardSnapshot();
            snapshot.shard = key;
            snapshot.windowStart = ctx.window().getStart();
            // 发出独立对象，后续窗口累加不能修改已发出的快照。
            snapshot.value = new CountActions().merge(new RankingAccumulator(), values.iterator().next());
            for (Map<String, ArticleScore> articles : snapshot.value.categories.values()) {
                for (ArticleScore article : articles.values()) snapshot.count += article.count;
            }
            out.collect(snapshot);
        }
    }

    private static class MergeRanking extends KeyedProcessFunction<Long, ShardSnapshot, JSONObject> {
        private transient MapState<String, ShardSnapshot> shards;
        private transient ValueState<Long> outputAt;

        @Override
        public void open(Configuration parameters) {
            shards = getRuntimeContext().getMapState(new MapStateDescriptor<>("b-latest-shards",
                    String.class, ShardSnapshot.class));
            outputAt = getRuntimeContext().getState(new ValueStateDescriptor<>("b-output-at", Long.class));
        }

        @Override
        public void processElement(ShardSnapshot value, Context ctx, Collector<JSONObject> out) throws Exception {
            ShardSnapshot previous = shards.get(value.shard);
            if (previous != null && previous.count > value.count) return;
            // 迟到触发携带累计值：替换旧分片，不是再次相加，防止重复计数。
            shards.put(value.shard, value);
            if (outputAt.value() == null) {
                long at = ctx.timerService().currentProcessingTime() + 1000;
                outputAt.update(at);
                ctx.timerService().registerProcessingTimeTimer(at);
            }
            long cleanup = value.windowStart + 600000 + 3900000;
            if (cleanup > ctx.timerService().currentWatermark()) ctx.timerService().registerEventTimeTimer(cleanup);
        }

        @Override
        public void onTimer(long time, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            RankingAccumulator combined = new RankingAccumulator();
            CountActions aggregator = new CountActions();
            for (ShardSnapshot snapshot : shards.values()) aggregator.merge(combined, snapshot.value);
            for (JSONObject row : ranking(ctx.getCurrentKey(), combined)) out.collect(row);
            Long at = outputAt.value();
            if (at != null) ctx.timerService().deleteProcessingTimeTimer(at);
            outputAt.clear();
            if (ctx.timeDomain() == org.apache.flink.streaming.api.TimeDomain.EVENT_TIME) shards.clear();
        }
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

    public static class CountActions implements AggregateFunction<JSONObject, RankingAccumulator, RankingAccumulator> {
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
                        ArticleScore copy = new ArticleScore();
                        copy.articleId = incoming.articleId;
                        copy.category = incoming.category;
                        copy.title = incoming.title;
                        copy.version = incoming.version;
                        copy.latestTime = incoming.latestTime;
                        copy.count = incoming.count;
                        target.put(incoming.articleId, copy);
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

    /** 两阶段在线聚合与明细补算使用同一份排序规则和输出格式。 */
    public static List<JSONObject> ranking(long start, RankingAccumulator accumulator) {
            Map<String, Map<String, ArticleScore>> grouped = accumulator.categories;
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
                row.put("window_start", Instant.ofEpochMilli(start).toString());
                row.put("window_end", Instant.ofEpochMilli(start + 600000).toString());
                row.put("detect_time", Instant.now().toString());
                row.put("revision", revision);
                rows.add(row);
                JSONObject snapshotRow = new JSONObject();
                snapshotRow.putAll(row);
                ranking.add(snapshotRow);
            }
            if (!rows.isEmpty()) rows.get(0).put("ranking", ranking);
            return rows;
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

package com.agd.flink.join;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import constant.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkMetricsUtil;
import util.FlinkRuntimeUtil;
import util.FlinkSinkUtil;
import util.FlinkSourceUtil;
import util.RoleStreamUtil;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.UUID;

/**
 * Kafka 双流清洗、去重、文章富化，三条旁路分别写入 MySQL。
 * 生成器保证同一 article_id 的行为事件时间不早于文章，不再等待首版发布或做跨流时间校验。
 * 到达顺序仍可能相反：先到行为暂存，文章到达后补 Join；迟到数据留痕后继续关联。
 */
public class ArticleJoinBehavior {
    private static final String JOIN_RECEIVED_AT_MS = "_join_received_at_ms";
    private static final String JOIN_SALT = "_join_salt";
    private static final long JOIN_RETENTION_MS = Duration.ofHours(2).toMillis();
    private static final long JOIN_ALLOWED_LATENESS_MS =3900000L;
    private static final Duration DISORDER = Duration.ofSeconds(30);

    private static final OutputTag<JSONObject> dirtyDataTag = new OutputTag<JSONObject>("dirty_data") {};
    private static final OutputTag<JSONObject> unmatchedBehaviorTag = new OutputTag<JSONObject>("unmatched_behavior") {};
    private static final OutputTag<JSONObject> lateDataTag = new OutputTag<JSONObject>("late_data") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        FlinkRuntimeUtil.configureCheckpointing(env, "HOTNEWS_CHECKPOINT_DIR");
        createJoinedStream(env, "hotnews-source").print("JOINED");
        env.execute("ArticleJoinBehavior");
    }

    /** 持续消费双流，返回富化行为；三条 Join 旁路在同一作业中直接写入 MySQL。 */
    public static SingleOutputStreamOperator<JSONObject> createJoinedStream(
            StreamExecutionEnvironment env, String groupPrefix) {
        return createJoinedStream(env, groupPrefix, 1);
    }

    /** 仅需热点拆分的入口传入盐数；默认值 1 保持独立规则原有的 Join 行为。 */
    public static SingleOutputStreamOperator<JSONObject> createJoinedStream(
            StreamExecutionEnvironment env, String groupPrefix, int joinSalts) {
        DataStream<String> articles = env.fromSource(FlinkSourceUtil.getKafkaSource(
                Constant.TOPIC_ARTICLE, groupPrefix + "-article", OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "Article_Source");
        DataStream<String> behaviors = env.fromSource(FlinkSourceUtil.getKafkaSource(
                Constant.TOPIC_BEHAVIOR, groupPrefix + "-behavior"),
                WatermarkStrategy.noWatermarks(), "Behavior_Source");
        SingleOutputStreamOperator<JSONObject> cleanArticles = etl(articles, true);
        SingleOutputStreamOperator<JSONObject> cleanBehaviors = etl(behaviors, false);
        SingleOutputStreamOperator<JSONObject> unique = deduplicateBehaviors(cleanBehaviors);
        SingleOutputStreamOperator<JSONObject> joined = joinCleanStreams(
                unique.assignTimestampsAndWatermarks(RoleStreamUtil.behaviorWatermarks()),
                cleanArticles.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(DISORDER)
                        .withTimestampAssigner((value, previous) -> eventTime(value))
                        .withIdleness(Duration.ofSeconds(30))), JOIN_RETENTION_MS, joinSalts);

        DataStream<JSONObject> dirty = cleanArticles.getSideOutput(dirtyDataTag)
                .union(cleanBehaviors.getSideOutput(dirtyDataTag));
        sinkSideOutputs(dirty, joined);
        return RoleStreamUtil.restoreBehaviorTime(joined);
    }

    public static SingleOutputStreamOperator<JSONObject> joinCleanStreams(
            DataStream<JSONObject> behaviors, DataStream<JSONObject> articles) {
        return joinCleanStreams(behaviors, articles, JOIN_RETENTION_MS);
    }

    public static SingleOutputStreamOperator<JSONObject> joinCleanStreams(
            DataStream<JSONObject> behaviors, DataStream<JSONObject> articles,
            long retentionMillis) {
        return joinCleanStreams(behaviors, articles, retentionMillis, 1);
    }

    public static SingleOutputStreamOperator<JSONObject> joinCleanStreams(
            DataStream<JSONObject> behaviors, DataStream<JSONObject> articles,
            long retentionMillis, int joinSalts) {
        if (joinSalts < 1) throw new IllegalArgumentException("joinSalts 必须大于 0");
        if (joinSalts == 1) {
            return behaviors.keyBy(value -> value.getString("article_id"))
                    .connect(articles.keyBy(value -> value.getString("article_id")))
                    .process(new ArticleBehaviorJoin(retentionMillis));
        }

        // 行为按 event_id 稳定选择一份文章副本；文章广播成 N 份后按复合 key 关联。
        // 上游事件时间与 Watermark 不变，每条行为依然只产出一次 Join 结果。
        DataStream<JSONObject> saltedBehaviors = behaviors.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject behavior) {
                JSONObject copy = new JSONObject();
                copy.putAll(behavior);
                copy.put(JOIN_SALT, Math.floorMod(behavior.getString("event_id").hashCode(), joinSalts));
                return copy;
            }
        });
        DataStream<JSONObject> replicatedArticles = articles.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
            @Override
            public void flatMap(JSONObject article, Collector<JSONObject> out) {
                for (int salt = 0; salt < joinSalts; salt++) {
                    JSONObject copy = new JSONObject();
                    copy.putAll(article);
                    copy.put(JOIN_SALT, salt);
                    out.collect(copy);
                }
            }
        });
        return saltedBehaviors.keyBy(value -> joinKey(value))
                .connect(replicatedArticles.keyBy(value -> joinKey(value)))
                .process(new ArticleBehaviorJoin(retentionMillis));
    }

    private static String joinKey(JSONObject value) {
        return value.getString("article_id") + "#" + value.getIntValue(JOIN_SALT);
    }

    public static OutputTag<JSONObject> unmatchedBehaviorTag() { return unmatchedBehaviorTag; }
    public static OutputTag<JSONObject> lateDataTag() { return lateDataTag; }

    public static SingleOutputStreamOperator<JSONObject> deduplicateBehaviors(DataStream<JSONObject> input) {
        return deduplicateBehaviors(input, Time.hours(24));
    }

    static SingleOutputStreamOperator<JSONObject> deduplicateBehaviors(DataStream<JSONObject> input, Time ttl) {
        return input.keyBy(value -> value.getString("event_id")).process(new BehaviorDeduplicate(ttl));
    }

    private static class BehaviorDeduplicate extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private final Time ttl;
        private transient ValueState<Boolean> seen;

        private BehaviorDeduplicate(Time ttl) { this.ttl = ttl; }

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<Boolean>("etl-seen-behavior-event-id", Boolean.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl).build());
            seen = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            if (seen.value() == null) {
                seen.update(true);
                out.collect(value);
            }
        }
    }

    /** 左流行为、右流文章。文章使用 TTL；待匹配行为到期先留回放记录，再清状态。 */
    private static class ArticleBehaviorJoin extends KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {
        private final long retentionMillis;
        private transient ValueState<JSONObject> articleState;
        private transient MapState<String, JSONObject> pending;
        private transient ValueState<Long> deadline;
        private transient Counter behaviorInput;
        private transient Counter articleInput;
        private transient Counter pendingInput;
        private transient Counter directMatches;
        private transient Counter rejoinedMatches;
        private transient FlinkMetricsUtil.RollingP95 joinLatency;

        private ArticleBehaviorJoin(long retentionMillis) {
            this.retentionMillis = retentionMillis;
        }

        @Override
        public void open(Configuration parameters) {
            org.apache.flink.metrics.MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("join");
            behaviorInput = metrics.counter("behavior_input");
            articleInput = metrics.counter("article_input");
            pendingInput = metrics.counter("pending_input");
            directMatches = metrics.counter("direct_matches");
            rejoinedMatches = metrics.counter("rejoined_matches");
            joinLatency = new FlinkMetricsUtil.RollingP95();
            metrics.gauge("etl_to_output_p95_ms", () -> joinLatency.value());
            ValueStateDescriptor<JSONObject> descriptor =
                    new ValueStateDescriptor<JSONObject>("article", JSONObject.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds(retentionMillis)).build());
            articleState = getRuntimeContext().getState(descriptor);
            pending = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, JSONObject>("pending-behaviors", String.class, JSONObject.class));
            deadline = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pending-deadline", Long.class));
        }

        @Override
        public void processElement1(JSONObject behavior, Context ctx, Collector<JSONObject> out) throws Exception {
            behaviorInput.inc();
            reportLate(behavior, "behavior", ctx);
            JSONObject article = articleState.value();
            if (article != null) {
                recordJoinLatency(behavior);
                directMatches.inc();
                out.collect(enrich(behavior, article));
                return;
            }
            String id = behavior.getString("event_id");
            if (pending.contains(id)) return;
            pending.put(id, behavior);
            pendingInput.inc();
            ctx.output(unmatchedBehaviorTag, unmatched(behavior, "ARTICLE_NOT_YET_FOUND", false));
            // 同一文章从第一条待匹配行为到达起等待两小时，整批到期后转入回放。
            if (deadline.value() == null) {
                long expires = ctx.timerService().currentProcessingTime() + retentionMillis;
                deadline.update(expires);
                ctx.timerService().registerProcessingTimeTimer(expires);
            }
        }

        @Override
        public void processElement2(JSONObject article, Context ctx, Collector<JSONObject> out) throws Exception {
            articleInput.inc();
            // 文章副本共用业务 event_id，迟到留痕只记录一次。
            if (!article.containsKey(JOIN_SALT) || article.getIntValue(JOIN_SALT) == 0) {
                reportLate(article, "article", ctx);
            }
            JSONObject previous = articleState.value();
            if (previous == null || article.getIntValue("version") >= previous.getIntValue("version")) {
                articleState.update(article);
            }
            JSONObject currentArticle = articleState.value();
            for (JSONObject behavior : pending.values()) {
                JSONObject result = enrich(behavior, currentArticle);
                recordJoinLatency(behavior);
                rejoinedMatches.inc();
                out.collect(result);
                ctx.output(unmatchedBehaviorTag, unmatched(result, "BEHAVIOR_REJOINED", true));
            }
            pending.clear();
            clearDeadline(ctx.timerService());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            if (!Long.valueOf(timestamp).equals(deadline.value())) return;
            for (JSONObject behavior : pending.values()) {
                ctx.output(unmatchedBehaviorTag,
                        unmatched(behavior, "ARTICLE_NOT_FOUND_REPLAY_REQUIRED", false));
            }
            pending.clear();
            clearDeadline(ctx.timerService());
        }

        private void clearDeadline(TimerService timers) throws Exception {
            Long expires = deadline.value();
            if (expires != null) timers.deleteProcessingTimeTimer(expires);
            deadline.clear();
        }

        private void reportLate(JSONObject value, String stream, Context ctx) {
            long watermark = ctx.timerService().currentWatermark();
            if (watermark != Long.MIN_VALUE && eventTime(value) < watermark - JOIN_ALLOWED_LATENESS_MS) {
                JSONObject late = mark(value, "BEYOND_JOIN_ALLOWED_LATENESS");
                late.put("stream", stream);
                late.put("watermark_ms", watermark);
                ctx.output(lateDataTag, late);
            }
            // 只旁路留痕，不丢弃：迟到行为仍关联，迟到文章仍触发待匹配行为补算。
        }

        private void recordJoinLatency(JSONObject behavior) {
            Long received = behavior.getLong(JOIN_RECEIVED_AT_MS);
            if (received != null) {
                joinLatency.record(Math.max(0, System.currentTimeMillis() - received));
            }
        }
    }

    private static JSONObject enrich(JSONObject behavior, JSONObject article) {
        JSONObject result = new JSONObject();
        result.putAll(behavior);
        result.remove(JOIN_RECEIVED_AT_MS);
        result.remove(JOIN_SALT);
        for (String field : new String[]{"title", "category", "tags"}) result.put(field, article.get(field));
        result.put("article_published_at", article.getString("published_at"));
        result.put("article_version", article.getInteger("version"));
        return result;
    }

    private static JSONObject mark(JSONObject value, String reason) {
        JSONObject result = new JSONObject();
        result.putAll(value);
        result.remove(JOIN_RECEIVED_AT_MS);
        result.remove(JOIN_SALT);
        result.put("dirty_reason", reason);
        return result;
    }

    private static JSONObject unmatched(JSONObject value, String reason, boolean matched) {
        JSONObject result = mark(value, reason);
        result.put("matched", matched);
        return result;
    }

    private static long eventTime(JSONObject value) {
        return parseTime(value.getString("event_time"));
    }

    private static long parseTime(String value) {
        return OffsetDateTime.parse(value).toInstant().toEpochMilli();
    }

    /** 校验失败原文进入 dirty_data。 */
    private static SingleOutputStreamOperator<JSONObject> etl(DataStream<String> input, boolean article) {
        return FlinkMetricsUtil.measure(input, article ? "source_article_records" : "source_behavior_records")
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String raw, Context ctx, Collector<JSONObject> out) {
                        JSONObject value;
                        try {
                            value = JSON.parseObject(raw);
                            validate(value, article);
                        } catch (RuntimeException error) {
                            JSONObject dirty = new JSONObject();
                            dirty.put("event_id", "raw-" + UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8)));
                            dirty.put("stream", article ? "article" : "behavior");
                            dirty.put("dirty_reason", "SCHEMA_VALIDATION_FAILED");
                            dirty.put("error_message", error.getMessage() == null
                                    ? "JSON、必填字段或时间非法" : error.getMessage());
                            dirty.put("payload_raw", raw);
                            ctx.output(dirtyDataTag, dirty);
                            return;
                        }
                        // 实际运行时钟用于观测 ETL 到 Join 输出的排队/等待，不参与事件时间计算。
                        if (!article) value.put(JOIN_RECEIVED_AT_MS, System.currentTimeMillis());
                        out.collect(value);
                    }
                });
    }

    private static void validate(JSONObject value, boolean article) {
        requireStrings(value, "event_id", "article_id", "event_time", "ingest_time");
        long eventTime = eventTime(value);
        long ingestTime = parseTime(value.getString("ingest_time"));
        if (eventTime > ingestTime + Duration.ofHours(2).toMillis()) {
            throw new IllegalArgumentException("event_time 超前 ingest_time 两小时以上");
        }
        if (article) {
            requireStrings(value, "event_type", "title", "category", "published_at");
            OffsetDateTime.parse(value.getString("published_at"));
            if (!Arrays.asList("publish", "update").contains(value.getString("event_type"))
                    || !(value.get("tags") instanceof JSONArray) || value.getJSONArray("tags").isEmpty()) {
                throw new IllegalArgumentException("文章 event_type 或 tags 非法");
            }
            requireInteger(value, "version", 1, Integer.MAX_VALUE);
        } else {
            requireStrings(value, "user_id", "action", "ip");
            if (!Arrays.asList("click", "share", "comment").contains(value.getString("action"))) {
                throw new IllegalArgumentException("行为 action 非法");
            }
            requireInteger(value, "read_duration_ms", 0, 86_400_000);
        }
    }

    private static void requireStrings(JSONObject value, String... fields) {
        for (String field : fields) {
            if (value == null || !(value.get(field) instanceof String) || value.getString(field).trim().isEmpty()) {
                throw new IllegalArgumentException("字段缺失或不是非空字符串: " + field);
            }
        }
    }

    private static void requireInteger(JSONObject value, String field, long min, long max) {
        Object number = value.get(field);
        if (!(number instanceof Integer || number instanceof Long)
                || ((Number) number).longValue() < min || ((Number) number).longValue() > max) {
            throw new IllegalArgumentException("整数字段缺失或超出范围: " + field);
        }
    }

    /** SQL 与建表文件一一对应，连接和写入复用现有 FlinkSinkUtil。 */
    private static void sinkSideOutputs(DataStream<JSONObject> dirty,
                                        SingleOutputStreamOperator<JSONObject> joined) {
        FlinkSinkUtil.sinkMySql(dirty,
                "INSERT INTO dirty_data(source_stream,record_id,reason,raw_data) VALUES(?,?,?,?) "
                        + "ON DUPLICATE KEY UPDATE record_id=VALUES(record_id)",
                (ps, value) -> {
                    ps.setString(1, value.getString("stream"));
                    ps.setString(2, value.getString("event_id"));
                    ps.setString(3, value.getString("error_message"));
                    ps.setString(4, value.getString("payload_raw"));
                }, 1, "MySQL dirty_data");

        FlinkSinkUtil.sinkMySql(joined.getSideOutput(lateDataTag),
                "INSERT INTO late_data(source_stream,event_id,article_id,event_time,watermark_ms,reason,payload) "
                        + "VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE event_id=VALUES(event_id)",
                (ps, value) -> {
                    ps.setString(1, value.getString("stream"));
                    ps.setString(2, value.getString("event_id"));
                    ps.setString(3, value.getString("article_id"));
                    ps.setString(4, value.getString("event_time"));
                    ps.setLong(5, value.getLongValue("watermark_ms"));
                    ps.setString(6, value.getString("dirty_reason"));
                    ps.setString(7, value.toJSONString());
                }, 1, "MySQL late_data");

        // 同一条旁路按顺序记录未匹配、等待回放和补算成功；旧记录不会倒退成未匹配。
        FlinkSinkUtil.sinkMySql(joined.getSideOutput(unmatchedBehaviorTag),
                "INSERT INTO unmatched_behavior(event_id,article_id,event_time,matched,reason,payload) "
                        + "VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE "
                        + "reason=IF(matched,reason,VALUES(reason)),payload=IF(matched,payload,VALUES(payload)),"
                        + "matched=GREATEST(matched,VALUES(matched))",
                (ps, value) -> {
                    ps.setString(1, value.getString("event_id"));
                    ps.setString(2, value.getString("article_id"));
                    ps.setString(3, value.getString("event_time"));
                    ps.setBoolean(4, value.getBooleanValue("matched"));
                    ps.setString(5, value.getString("dirty_reason"));
                    ps.setString(6, value.toJSONString());
                }, 1, "MySQL unmatched_behavior");

        // 历史行为重新消费时文章可能已在状态中，只更新表内已有的未匹配记录。
        FlinkSinkUtil.sinkMySql(joined,
                "UPDATE unmatched_behavior SET matched=TRUE,reason='BEHAVIOR_REJOINED' WHERE event_id=?",
                (ps, value) -> ps.setString(1, value.getString("event_id")),
                null, "MySQL matched behavior");
    }
}

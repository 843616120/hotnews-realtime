import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import constant.Constant;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSourceUtil;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 文章与行为双流关联。用法：三个规则都复用此流；有界验收从两条 Topic 起点回放。
 * 思路：先做单流 Schema ETL 和行为 event_id 去重，再分别生成水位线；
 * 在按文章键分组的时序 ETL 中
 * 校验首版发布时间；行为先到立即报告未匹配，同时留在状态中待文章到达后补 Join。
 * 超时迟到只进入可查询旁路，不会阻止合法行为重新关联。
 */
public class ArticleJoinBehavior {
    private static final long FINAL_AUDIT_TIMER = Long.MAX_VALUE - 1;
    private static final long JOIN_RETENTION_MS = Duration.ofHours(2).toMillis();
    private static final long JOIN_ALLOWED_LATENESS_MS = Duration.ofSeconds(30).toMillis();
    private static final Duration ARTICLE_DISORDER = Duration.ofSeconds(30);
    private static final Duration BEHAVIOR_DISORDER = Duration.ofMinutes(65);
    private static final Duration BOUNDED_REPLAY_DISORDER = Duration.ofHours(2);
    private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(30);

    /** 分别接入文章、行为 Topic，校验后按 article_id 关联并输出正常及旁路结果。 */
    public static void main(String[] args) throws Exception {
        boolean bounded = args.length == 1 && "--bounded".equals(args[0]);
        if (args.length > 0 && !bounded) {
            throw new IllegalArgumentException("仅支持 --bounded 验收参数");
        }
        //TODO 1.基本环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);

        //TODO 2.检查点相关设置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<JSONObject> joined = createJoinedStream(
                env, bounded ? "hotnews-join-verify" : "hotnews-source", bounded, bounded);
        if (bounded) {
            joined.map(value -> value.getString("event_id")).print("JOINED_ID");
            joined.keyBy(value -> "all")
                    .process(new CountJoined()).print("JOINED_TOTAL");
        } else {
            joined.print("JOINED");
        }

        //TODO 7.启动执行
        env.execute("ArticleJoinBehavior");
    }

    /** 每个规则使用独立消费组，复用同一套双流关联逻辑。 */
    public static SingleOutputStreamOperator<JSONObject> createJoinedStream(
            StreamExecutionEnvironment env, String groupPrefix) {
        return createJoinedStream(env, groupPrefix, false);
    }

    public static SingleOutputStreamOperator<JSONObject> createJoinedStream(
            StreamExecutionEnvironment env, String groupPrefix, boolean bounded) {
        return createJoinedStream(env, groupPrefix, bounded, false);
    }

    /**
     * 有界验收从最早位点回放两条 Topic；持续消费时文章流总从起点重建维表，
     * 行为流沿用消费组位点。两种模式都等待首次发布事件完成关联。
     */
    public static SingleOutputStreamOperator<JSONObject> createJoinedStream(
            StreamExecutionEnvironment env, String groupPrefix, boolean bounded, boolean fullReplay) {
        if (fullReplay && !bounded) {
            throw new IllegalArgumentException("完整回放仅支持有界输入");
        }
        //TODO 3.读取KafkaSource 并封装为流
        KafkaSource<String> articleSource = FlinkSourceUtil.getKafkaSource(
                Constant.TOPIC_ARTICLE, groupPrefix + "-article", bounded, true);
        KafkaSource<String> behaviorSource = FlinkSourceUtil.getKafkaSource(
                Constant.TOPIC_BEHAVIOR, groupPrefix + "-behavior", bounded, fullReplay);
        DataStreamSource<String> articleStream = env.fromSource(
                articleSource, WatermarkStrategy.noWatermarks(), "Article_Source");
        DataStreamSource<String> behaviorStream = env.fromSource(
                behaviorSource, WatermarkStrategy.noWatermarks(), "Behavior_Source");
        //TODO 3.先分别清洗 Schema；原始字符串尚无可靠事件时间，清洗后再生成水位线。
        SingleOutputStreamOperator<JSONObject> ArticleDS = etl(articleStream);
        SingleOutputStreamOperator<JSONObject> BehaviorDS = etl(behaviorStream);
        ArticleDS.getSideOutput(dirtyDataTag).print("DIRTY_ARTICLE");
        BehaviorDS.getSideOutput(dirtyDataTag).print("DIRTY_BEHAVIOR");

        //TODO 4.在 ETL 末尾先去重，后分配 Watermark；迟到事件留痕并继续 Join。
        SingleOutputStreamOperator<JSONObject> deduplicated = deduplicateBehaviors(BehaviorDS);
        deduplicated.getSideOutput(duplicateBehaviorTag).print("DUPLICATE_BEHAVIOR");
        SingleOutputStreamOperator<JSONObject> joinedStream =
                joinCleanStreams(withWatermarks(deduplicated,
                                bounded ? BOUNDED_REPLAY_DISORDER : BEHAVIOR_DISORDER),
                        withWatermarks(ArticleDS, ARTICLE_DISORDER), bounded);
        joinedStream.getSideOutput(dirtyBehaviorTag).print("DIRTY_BEHAVIOR_ETL");
        joinedStream.getSideOutput(unmatchedBehaviorTag).print("UNMATCHED_BEHAVIOR");
        joinedStream.getSideOutput(lateDataTag).print("LATE_DATA");
        if (bounded) {
            joinedStream.getSideOutput(rejoinedBehaviorTag).print("REJOINED_BEHAVIOR");
        }
        joinedStream.getSideOutput(replayRequiredTag).print("REPLAY_REQUIRED");

        return joinedStream;
    }

    /** 已完成 Schema 清洗的双流直接关联；生产入口已分配水位线，测试可注入水位线。 */
    public static SingleOutputStreamOperator<JSONObject> joinCleanStreams(
            DataStream<JSONObject> behaviors, DataStream<JSONObject> articles, boolean bounded) {
        return joinCleanStreams(behaviors, articles, bounded, JOIN_RETENTION_MS);
    }

    /** 测试可缩短待匹配保留时间，验证到期后明确进入回放队列。 */
    static SingleOutputStreamOperator<JSONObject> joinCleanStreams(
            DataStream<JSONObject> behaviors, DataStream<JSONObject> articles,
            boolean bounded, long retentionMillis) {
        if (retentionMillis <= 0) {
            throw new IllegalArgumentException("待匹配保留时间必须大于 0");
        }
        return behaviors
                .keyBy(behavior -> behavior.getString("article_id"))
                .connect(articles.keyBy(article -> article.getString("article_id")))
                .process(new ArticleBehaviorJoin(bounded, retentionMillis));
    }

    /** 跨流时序 ETL 判定早于最初发布的行为进入此脏数据旁路。 */
    public static OutputTag<JSONObject> dirtyBehaviorTag() {
        return dirtyBehaviorTag;
    }

    /** 行为先到时立即输出此旁路；并不表示永久无法 Join。 */
    public static OutputTag<JSONObject> unmatchedBehaviorTag() {
        return unmatchedBehaviorTag;
    }

    /** 超过 Join 允许迟到时间的文章或行为，同时仍参与关联。 */
    public static OutputTag<JSONObject> lateDataTag() {
        return lateDataTag;
    }

    /** 等待状态到期或有界回放结束仍未匹配，需要从原始 Topic 回放。 */
    public static OutputTag<JSONObject> replayRequiredTag() {
        return replayRequiredTag;
    }

    /** 有界审计时按 event_id 核对“先未匹配、后补 Join”的事件。 */
    public static OutputTag<JSONObject> rejoinedBehaviorTag() {
        return rejoinedBehaviorTag;
    }

    /** Schema 清洗后的 ETL 去重；测试可自定义 TTL，生产默认 24 小时。 */
    public static SingleOutputStreamOperator<JSONObject> deduplicateBehaviors(DataStream<JSONObject> behaviors) {
        return deduplicateBehaviors(behaviors, Time.hours(24));
    }

    static SingleOutputStreamOperator<JSONObject> deduplicateBehaviors(
            DataStream<JSONObject> behaviors, Time ttl) {
        return behaviors.keyBy(value -> value.getString("event_id"))
                .process(new BehaviorDeduplicate(ttl));
    }

    /** 清洗后才提取事件时间；每条流独立等待乱序，闲置分区不阻塞下游水位线。 */
    private static SingleOutputStreamOperator<JSONObject> withWatermarks(
            DataStream<JSONObject> stream, Duration disorder) {
        return stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(disorder)
                        .withTimestampAssigner((value, previous) -> eventTime(value))
                        .withIdleness(IDLE_TIMEOUT));
    }

    //JSON解析失败 字段缺失 时间非法的数据进入dirty_data旁路流
    private static final OutputTag<String> dirtyDataTag = new OutputTag<String>("dirty_data") {};
    private static final OutputTag<JSONObject> dirtyBehaviorTag =
            new OutputTag<JSONObject>("dirty_data_temporal") {};
    private static final OutputTag<JSONObject> unmatchedBehaviorTag = new OutputTag<JSONObject>("unmatched_behavior") {};
    private static final OutputTag<JSONObject> lateDataTag = new OutputTag<JSONObject>("late_data") {};
    private static final OutputTag<JSONObject> replayRequiredTag = new OutputTag<JSONObject>("replay_required") {};
    private static final OutputTag<JSONObject> rejoinedBehaviorTag = new OutputTag<JSONObject>("rejoined_behavior") {};
    private static final OutputTag<JSONObject> duplicateBehaviorTag =
            new OutputTag<JSONObject>("duplicate_behavior") {};

    /** 行为 ETL 去重：按 event_id 存 24 小时处理时间状态，重复副本仅进入重复旁路。 */
    private static class BehaviorDeduplicate extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private final Time ttl;
        private transient ValueState<Boolean> seen;

        private BehaviorDeduplicate(Time ttl) {
            this.ttl = ttl;
        }

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<Boolean>("etl-seen-behavior-event-id", Boolean.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl).build());
            seen = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out)
                throws Exception {
            if (seen.value() == null) {
                seen.update(true);
                out.collect(value);
            } else {
                ctx.output(duplicateBehaviorTag, value);
            }
        }
    }

    /** 按文章键做时序 ETL 与富化；左行为、右文章，迟到旁路与主流独立输出。 */
    private static class ArticleBehaviorJoin extends KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {
        private final boolean bounded;
        private final long retentionMillis;
        private transient ValueState<JSONObject> articleState;
        private transient ValueState<Long> initialPublication;
        private transient MapState<String, JSONObject> pendingBehaviors;
        private transient MapState<String, Long> pendingExpirations;
        private transient ValueState<Long> pendingDeadline;

        private ArticleBehaviorJoin(boolean bounded, long retentionMillis) {
            this.bounded = bounded;
            this.retentionMillis = retentionMillis;
        }

        /** 文章维度 2 小时 TTL；待匹配行为靠显式定时器清理，不允许 TTL 静默清除。 */
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<JSONObject> articleDescriptor =
                    new ValueStateDescriptor<JSONObject>("article", JSONObject.class);
            StateTtlConfig articleTtl = StateTtlConfig.newBuilder(Time.hours(2)).build();
            articleDescriptor.enableTimeToLive(articleTtl);
            articleState = getRuntimeContext().getState(articleDescriptor);
            ValueStateDescriptor<Long> publicationDescriptor =
                    new ValueStateDescriptor<Long>("initial-publication", Long.class);
            publicationDescriptor.enableTimeToLive(articleTtl);
            initialPublication = getRuntimeContext().getState(publicationDescriptor);
            pendingBehaviors = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, JSONObject>(
                            "pending-behaviors-by-event-id", String.class, JSONObject.class));
            pendingExpirations = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Long>(
                            "pending-expirations-by-event-id", String.class, Long.class));
            pendingDeadline = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("pending-deadline", Long.class));
        }

        /** 行为先到立即报告未匹配，ETL 已先去重，待匹配状态只需保存一份。 */
        @Override
        public void processElement1(JSONObject behavior, Context ctx, Collector<JSONObject> out) throws Exception {
            reportLate(behavior, "behavior", ctx);
            Long published = initialPublication.value();
            if (published != null) {
                if (eventTime(behavior) < published) {
                    ctx.output(dirtyBehaviorTag, dirtyBehavior(behavior, "BEFORE_INITIAL_PUBLICATION"));
                } else {
                    out.collect(enrich(behavior, articleState.value()));
                }
            } else {
                String id = behavior.getString("event_id");
                if (pendingBehaviors.contains(id)) {
                    return;
                }
                pendingBehaviors.put(id, behavior);
                ctx.output(unmatchedBehaviorTag, dirtyBehavior(behavior,
                        articleState.value() == null ? "ARTICLE_NOT_YET_FOUND"
                                : "INITIAL_PUBLICATION_NOT_YET_FOUND"));
                long deadline = ctx.timerService().currentProcessingTime() + retentionMillis;
                pendingExpirations.put(id, deadline);
                if (pendingDeadline.value() == null) {
                    pendingDeadline.update(deadline);
                    ctx.timerService().registerProcessingTimeTimer(deadline);
                }
                if (bounded) {
                    ctx.timerService().registerEventTimeTimer(FINAL_AUDIT_TIMER);
                }
            }
        }

        /** 右流是文章流：最初发布版本决定有效时间下界，再处理此前等待的行为。 */
        @Override
        public void processElement2(JSONObject article, Context ctx, Collector<JSONObject> out) throws Exception {
            reportLate(article, "article", ctx);
            if ("publish".equals(article.getString("event_type"))
                    && article.getIntValue("version") == 1) {
                Long previousPublish = initialPublication.value();
                if (previousPublish == null || eventTime(article) < previousPublish) {
                    initialPublication.update(eventTime(article));
                }
            }
            JSONObject previous = articleState.value();
            if (previous == null || article.getIntValue("version") >= previous.getIntValue("version")) {
                articleState.update(article);
            }
            if (initialPublication.value() == null) {
                return;
            }
            for (JSONObject behavior : pendingBehaviors.values()) {
                if (eventTime(behavior) < initialPublication.value()) {
                    ctx.output(dirtyBehaviorTag, dirtyBehavior(behavior, "BEFORE_INITIAL_PUBLICATION"));
                } else {
                    JSONObject enriched = enrich(behavior, articleState.value());
                    if (bounded) {
                        ctx.output(rejoinedBehaviorTag, enriched);
                    }
                    out.collect(enriched);
                }
            }
            pendingBehaviors.clear();
            pendingExpirations.clear();
            clearDeadline(ctx.timerService());
        }

        /** 状态保留到期或有界回放结束，显式报告需从原始 Topic 补算的行为。 */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME
                    && !Long.valueOf(timestamp).equals(pendingDeadline.value())) {
                return;
            }
            if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
                List<String> expired = new ArrayList<String>();
                long next = Long.MAX_VALUE;
                for (Map.Entry<String, Long> entry : pendingExpirations.entries()) {
                    if (entry.getValue() <= timestamp) {
                        expired.add(entry.getKey());
                    } else {
                        next = Math.min(next, entry.getValue());
                    }
                }
                for (String id : expired) {
                    ctx.output(replayRequiredTag, dirtyBehavior(pendingBehaviors.get(id),
                            articleState.value() == null ? "ARTICLE_NOT_FOUND"
                                    : "INITIAL_PUBLICATION_NOT_FOUND"));
                    pendingBehaviors.remove(id);
                    pendingExpirations.remove(id);
                }
                pendingDeadline.clear();
                if (next != Long.MAX_VALUE) {
                    pendingDeadline.update(next);
                    ctx.timerService().registerProcessingTimeTimer(next);
                }
                return;
            }
            if (!pendingBehaviors.isEmpty()) {
                for (JSONObject behavior : pendingBehaviors.values()) {
                    ctx.output(replayRequiredTag, dirtyBehavior(behavior,
                            articleState.value() == null ? "ARTICLE_NOT_FOUND"
                                    : "INITIAL_PUBLICATION_NOT_FOUND"));
                }
                pendingBehaviors.clear();
            }
            pendingExpirations.clear();
            clearDeadline(ctx.timerService());
        }

        /** 清除已成功重关联的等待超时定时器。 */
        private void clearDeadline(org.apache.flink.streaming.api.TimerService timers) throws Exception {
            Long deadline = pendingDeadline.value();
            if (deadline != null) {
                timers.deleteProcessingTimeTimer(deadline);
                pendingDeadline.clear();
            }
        }

        /** 迟到记录先旁路留痕，再继续走时序 ETL/Join，不静默丢弃。 */
        private void reportLate(JSONObject value, String stream, Context ctx) {
            long watermark = ctx.timerService().currentWatermark();
            if (watermark != Long.MIN_VALUE
                    && eventTime(value) < watermark - JOIN_ALLOWED_LATENESS_MS) {
                JSONObject late = dirtyBehavior(value, "BEYOND_JOIN_ALLOWED_LATENESS");
                late.put("stream", stream);
                ctx.output(lateDataTag, late);
            }
        }
    }

    /** 加入原因但不改变原事件，以便区分 Schema 脏数据与早于首次发布的行为。 */
    private static JSONObject dirtyBehavior(JSONObject behavior, String reason) {
        JSONObject copy = new JSONObject();
        copy.putAll(behavior);
        copy.put("dirty_reason", reason);
        return copy;
    }

    /** 将已校验的 ISO-8601 event_time 统一转成毫秒，供状态关联和定时器比较。 */
    private static long eventTime(JSONObject value) {
        return OffsetDateTime.parse(value.getString("event_time")).toInstant().toEpochMilli();
    }

    /** 保留原行为字段，并从同一 article_id 的文章状态补充标题、分类和标签。 */
    private static JSONObject enrich(JSONObject behavior, JSONObject article) {
        JSONObject enrichedBehavior = new JSONObject();
        enrichedBehavior.put("event_id", behavior.getString("event_id"));
        enrichedBehavior.put("user_id", behavior.getString("user_id"));
        enrichedBehavior.put("article_id", behavior.getString("article_id"));
        enrichedBehavior.put("action", behavior.getString("action"));
        enrichedBehavior.put("ip", behavior.getString("ip"));
        enrichedBehavior.put("event_time", behavior.getString("event_time"));
        enrichedBehavior.put("ingest_time", behavior.getString("ingest_time"));
        enrichedBehavior.put("read_duration_ms", behavior.getInteger("read_duration_ms"));
        enrichedBehavior.put("title", article.getString("title"));
        enrichedBehavior.put("category", article.getString("category"));
        enrichedBehavior.put("tags", article.getJSONArray("tags"));
        enrichedBehavior.put("article_published_at", article.getString("published_at"));
        enrichedBehavior.put("article_version", article.getInteger("version"));
        return enrichedBehavior;
    }

    /** 两个 Kafka Source 分别调用此方法；解析失败或不符合对应 Schema 的记录进脏数据旁路。 */
    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> source) {
        return source.process(new ProcessFunction<String, JSONObject>() {
            /** 用文章特有的 event_type 选择校验规则；这不是 Topic 身份鉴别，脏数据归属由 Source 决定。 */
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                try {
                    // 第一步：把 Kafka 中的字符串解析为 JSON 对象。
                    JSONObject jsonObject = JSON.parseObject(value);

                    // 文章记录有 event_type (publish/update)，行为记录没有它而使用 action。
                    // 这是针对两种 Schema 的简化识别；缺少 event_type 的文章通常也会因缺行为字段而进入脏数据。
                    if (jsonObject.containsKey("event_type")) {
                        validateArticle(jsonObject);
                    } else {
                        validateBehavior(jsonObject);
                    }

                    // 第三步：解析和校验都通过，进入 ETL 后的正常流。
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 第四步：解析失败或字段校验失败，原始数据直接进入 dirty_data。
                    ctx.output(dirtyDataTag, value);
                }
            }
        });
    }

    /** 校验文章流独有的 event_type、标题、分类、标签、版本和必需时间字段。 */
    private static void validateArticle(JSONObject jsonObject) {
        requirePattern(jsonObject, "event_id", "^article-event-[0-9]{8}$");
        requireEnum(jsonObject, "event_type", "publish", "update");
        requirePattern(jsonObject, "article_id", "^article-[0-9]{6}$");
        requireStringLength(jsonObject, "title", 1, 200);
        requireStringLength(jsonObject, "category", 1, 64);
        validateTags(jsonObject.get("tags"));
        validateDateTime(jsonObject, "published_at");
        validateDateTime(jsonObject, "event_time");
        validateDateTime(jsonObject, "ingest_time");
        validateFutureEventTime(jsonObject);
        requireIntegerRange(jsonObject, "version", 1, Integer.MAX_VALUE);
    }

    /** 校验行为流独有的 user_id、action、IP、阅读时长及必需时间字段。 */
    private static void validateBehavior(JSONObject jsonObject) {
        requirePattern(jsonObject, "event_id", "^behavior-event-[0-9]{8}$");
        requirePattern(jsonObject, "user_id", "^user-[0-9]{6}$");
        requirePattern(jsonObject, "article_id", "^article-[0-9]{6}$");
        requireEnum(jsonObject, "action", "click", "share", "comment");
        requirePattern(jsonObject, "ip", "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");
        validateDateTime(jsonObject, "event_time");
        validateDateTime(jsonObject, "ingest_time");
        validateFutureEventTime(jsonObject);
        requireIntegerRange(jsonObject, "read_duration_ms", 0, 86400000);
    }

    /** 必填字段必须是真正的非空字符串，不能把 null 或数字当作有效值。 */
    private static void requireString(JSONObject jsonObject, String field) {
        Object value = jsonObject.get(field);
        if (!(value instanceof String) || ((String) value).length() == 0) {
            throw new IllegalArgumentException();
        }
    }

    /** 在非空字符串的前提下检查 Schema 规定的最短和最长长度。 */
    private static void requireStringLength(JSONObject jsonObject, String field, int min, int max) {
        requireString(jsonObject, field);
        int length = jsonObject.getString(field).length();
        if (length < min || length > max) {
            throw new IllegalArgumentException();
        }
    }

    /** 验证 event_id、article_id、user_id、IP 等字符串的完整格式。 */
    private static void requirePattern(JSONObject jsonObject, String field, String pattern) {
        requireString(jsonObject, field);
        if (!jsonObject.getString(field).matches(pattern)) {
            throw new IllegalArgumentException();
        }
    }

    /** 仅允许 Schema 列出的文章事件类型或用户行为类型。 */
    private static void requireEnum(JSONObject jsonObject, String field, String... values) {
        requireString(jsonObject, field);
        for (String value : values) {
            if (value.equals(jsonObject.getString(field))) {
                return;
            }
        }
        throw new IllegalArgumentException();
    }

    /** 整数值须落在给定范围内，排除浮点数和缺失字段。 */
    private static void requireIntegerRange(JSONObject jsonObject, String field, int min, int max) {
        Object value = jsonObject.get(field);
        if (!(value instanceof Number) || value instanceof Float || value instanceof Double
                || ((Number) value).longValue() < min || ((Number) value).longValue() > max) {
            throw new IllegalArgumentException();
        }
    }

    /** 时间字段必须是可解析、含时区偏移的 ISO-8601 字符串。 */
    private static void validateDateTime(JSONObject jsonObject, String field) {
        requireString(jsonObject, field);
        OffsetDateTime.parse(jsonObject.getString(field));
    }

    /** 按固定数据的到达时间校验：事件时间超前 ingest_time 两小时以上视为脏数据。 */
    private static void validateFutureEventTime(JSONObject jsonObject) {
        if (eventTime(jsonObject) > OffsetDateTime.parse(jsonObject.getString("ingest_time"))
                .toInstant().plus(Duration.ofHours(2)).toEpochMilli()) {
            throw new IllegalArgumentException();
        }
    }

    /** 有界关联核对统计 Schema 清洗且已在 ETL 去重的有效行为。 */
    private static class CountJoined extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<
            String, JSONObject, Long> {
        private transient ValueState<Long> count;

        @Override
        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("joined-count", Long.class));
        }

        @Override
        public void processElement(JSONObject value, Context context, Collector<Long> out) throws Exception {
            Long current = count.value();
            count.update(current == null ? 1L : current + 1);
            context.timerService().registerEventTimeTimer(FINAL_AUDIT_TIMER);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out) throws Exception {
            out.collect(count.value());
        }
    }

    /** 标签必须是 1-10 个不重复的非空短字符串，符合文章 Schema。 */
    private static void validateTags(Object value) {
        if (!(value instanceof JSONArray)) {
            throw new IllegalArgumentException();
        }
        JSONArray tags = (JSONArray) value;
        if (tags.size() < 1 || tags.size() > 10) {
            throw new IllegalArgumentException();
        }
        Set<String> uniqueTags = new HashSet<String>();
        for (Object tag : tags) {
            if (!(tag instanceof String) || ((String) tag).length() < 1
                    || ((String) tag).length() > 32 || !uniqueTags.add((String) tag)) {
                throw new IllegalArgumentException();
            }
        }
    }
}

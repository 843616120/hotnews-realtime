import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import constant.Constant;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkSourceUtil;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ArticleJoinBehavior {
    private static final long ALLOWED_LATENESS_MS = Duration.ofSeconds(30).toMillis();
    private static final Duration JOIN_TTL = Duration.ofHours(2);
    /*
     * 固定生成器样本中，行为事件时间最大回退约为 3625 秒。
     * 65 分钟覆盖行为提前到达造成的事件时间乱序，并保留少量边界余量。
     */
    private static final Duration EVENT_TIME_OUT_OF_ORDERNESS = Duration.ofMinutes(65);

    /** 分别接入文章、行为 Topic，校验后按 article_id 关联并输出正常及旁路结果。 */
    public static void main(String[] args) throws Exception {

        //TODO 1.基本环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);

        //TODO 2.检查点相关设置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //TODO 3.读取KafkaSource 并封装为流
        KafkaSource<String> ArticleSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_ARTICLE, "hotnews-source-article");
        KafkaSource<String> BehaviorSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_BEHAVIOR, "hotnews-source-behavior");
        DataStreamSource<String> ArticleStream = env.fromSource(ArticleSource, WatermarkStrategy.noWatermarks(), "Article_Source");
        DataStreamSource<String> BehaviorStream = env.fromSource(BehaviorSource, WatermarkStrategy.noWatermarks(), "Behavior_Source");
        //TODO 3.etl
        SingleOutputStreamOperator<JSONObject> ArticleDS = etl(ArticleStream);
        SingleOutputStreamOperator<JSONObject> BehaviorDS = etl(BehaviorStream);

        ArticleDS.getSideOutput(dirtyDataTag).print("DIRTY_ARTICLE");
        BehaviorDS.getSideOutput(dirtyDataTag).print("DIRTY_BEHAVIOR");
        //TODO 4.设定水位线
        SingleOutputStreamOperator<JSONObject> ArticleDSWithWatermark = ArticleDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    /** 使用文章的 event_time（发布或更新时刻），不以 ingest_time 生成 Watermark。 */
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        String eventTimeStr = element.getString("event_time");
                                        return OffsetDateTime.parse(eventTimeStr).toInstant().toEpochMilli();
                                    }
                                }
                        )
                        .withIdleness(Duration.ofSeconds(30))
        );

        SingleOutputStreamOperator<JSONObject> BehaviorDSWithWatermark = BehaviorDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(EVENT_TIME_OUT_OF_ORDERNESS)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    /** 行为发生时间作为事件时间，用于判断关联和迟到。 */
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        String eventTimeStr = element.getString("event_time");
                                        return OffsetDateTime.parse(eventTimeStr).toInstant().toEpochMilli();
                                    }
                                }
                        )
                        .withIdleness(Duration.ofSeconds(30))
        );

        //TODO 5.双流Join - 按article_id关联
        // 行为流可能先到；暂存到文章到达或事件时间超过等待期限。
        SingleOutputStreamOperator<JSONObject> joinedStream = BehaviorDSWithWatermark
                .keyBy(behavior -> behavior.getString("article_id"))
                .connect(ArticleDSWithWatermark.keyBy(article -> article.getString("article_id")))
                .process(new ArticleBehaviorJoin());

        //TODO 6.输出富化后的流

        joinedStream.print("JOINED");
        joinedStream.getSideOutput(lateDataTag).print("LATE_DATA");
        joinedStream.getSideOutput(unmatchedBehaviorTag).print("UNMATCHED_BEHAVIOR");

        //TODO 7.启动执行
        env.execute("ArticleJoinBehavior");


    }
    //JSON解析失败 字段缺失 时间非法的数据进入dirty_data旁路流
    private static final OutputTag<String> dirtyDataTag = new OutputTag<String>("dirty_data") {};
    private static final OutputTag<JSONObject> lateDataTag = new OutputTag<JSONObject>("late_data") {};
    private static final OutputTag<JSONObject> unmatchedBehaviorTag = new OutputTag<JSONObject>("unmatched_behavior") {};

    /** 两条已按 article_id 分组的流共享同一个 key：左侧是行为，右侧是文章。 */
    private static class ArticleBehaviorJoin extends KeyedCoProcessFunction<String, JSONObject, JSONObject, JSONObject> {
        private transient ValueState<JSONObject> articleState;
        private transient ListState<JSONObject> pendingBehaviors;

        /** 每个 article_id 保存最新文章；先到的行为单独暂存，等待文章或定时器。 */
        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<JSONObject> articleDescriptor =
                    new ValueStateDescriptor<JSONObject>("article", JSONObject.class);
            articleDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(2)).build());
            articleState = getRuntimeContext().getState(articleDescriptor);
            pendingBehaviors = getRuntimeContext().getListState(
                    new ListStateDescriptor<JSONObject>("pending-behaviors", JSONObject.class));
        }

        /** 左流是行为流：过迟的进旁路；有文章则富化，否则暂存并预约未匹配检查。 */
        @Override
        public void processElement1(JSONObject behavior, Context ctx, Collector<JSONObject> out) throws Exception {
            long eventTime = eventTime(behavior);
            if (isLate(eventTime, ctx.timerService().currentWatermark())) {
                ctx.output(lateDataTag, behavior);
                return;
            }
            JSONObject article = articleState.value();
            if (article != null && eventTime >= eventTime(article)) {
                out.collect(enrich(behavior, article));
            } else {
                pendingBehaviors.add(behavior);
                ctx.timerService().registerEventTimeTimer(eventTime + ALLOWED_LATENESS_MS);
            }
        }

        /** 右流是文章流：保存不旧于当前版本的文章，并补齐此前先到的行为。 */
        @Override
        public void processElement2(JSONObject article, Context ctx, Collector<JSONObject> out) throws Exception {
            if (isLate(eventTime(article), ctx.timerService().currentWatermark())) {
                ctx.output(lateDataTag, article);
                return;
            }
            JSONObject previous = articleState.value();
            if (previous != null && article.getInteger("version") < previous.getInteger("version")) {
                return;
            }
            articleState.update(article);
            List<JSONObject> remaining = new ArrayList<JSONObject>();
            Iterable<JSONObject> pending = pendingBehaviors.get();
            if (pending != null) {
                for (JSONObject behavior : pending) {
                    if (eventTime(behavior) >= eventTime(article)) {
                        out.collect(enrich(behavior, article));
                    } else {
                        remaining.add(behavior);
                    }
                }
            }
            pendingBehaviors.update(remaining);
        }

        /** 水位线越过等待期限后，将仍未找到文章的行为输出到 unmatched_behavior。 */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            List<JSONObject> remaining = new ArrayList<JSONObject>();
            Iterable<JSONObject> pending = pendingBehaviors.get();
            if (pending != null) {
                for (JSONObject behavior : pending) {
                    if (eventTime(behavior) + ALLOWED_LATENESS_MS <= timestamp) {
                        ctx.output(unmatchedBehaviorTag, behavior);
                    } else {
                        remaining.add(behavior);
                    }
                }
            }
            pendingBehaviors.update(remaining);
        }
    }

    /** 将已校验的 ISO-8601 event_time 统一转成毫秒，供状态关联和定时器比较。 */
    private static long eventTime(JSONObject value) {
        return OffsetDateTime.parse(value.getString("event_time")).toInstant().toEpochMilli();
    }

    /** 允许事件时间落后当前水位线最多 30 秒；初始水位线是最小 long，不能直接相减。 */
    private static boolean isLate(long eventTime, long watermark) {
        return watermark != Long.MIN_VALUE && eventTime < watermark - ALLOWED_LATENESS_MS;
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
                .toInstant().plus(JOIN_TTL).toEpochMilli()) {
            throw new IllegalArgumentException();
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

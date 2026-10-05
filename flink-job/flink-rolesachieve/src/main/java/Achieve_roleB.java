import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则 B：每十分钟统计分类热度前五及分类内的前五篇文章。
 *
 * <p>用法：正常启动持续消费；传入 --bounded 从两条 Kafka Topic 的最早位点回放。
 * 思路：先按文章键保存窗口累计值，窗口关闭后仍可发送修正后的累计值；
 * 再按窗口键覆盖同一文章的旧值，重新输出完整的五名榜单。
 * 同一窗口和 rank 的最后一条结果是当前版本；晚到更新按水位线每五秒合并一次，
 * 窗口结束后 24 小时停止补算。</p>
 */
public class Achieve_roleB {
    private static final long WINDOW_MS = 600_000L;
    private static final long RETENTION_MS = 86_400_000L;
    private static final long CORRECTION_INTERVAL_MS = 5_000L;
    private static final OutputTag<JSONObject> inputLateTag =
            new OutputTag<JSONObject>("role-b-input-late") {};
    private static final OutputTag<ArticleScore> lateTag =
            new OutputTag<ArticleScore>("role-b-late") {};

    /** 共用 Sink 作业订阅排名阶段无法再补算的文章得分。 */
    public static OutputTag<ArticleScore> lateTag() {
        return lateTag;
    }

    public static void main(String[] args) throws Exception {
        boolean bounded = args.length == 1 && "--bounded".equals(args[0]);
        if (args.length > 0 && !bounded) {
            throw new IllegalArgumentException("仅支持 --bounded 验收参数");
        }
        //TODO 1.准备环境和检查点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(3);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        //TODO 2.回放模式等待全部文章到达；实时模式继续使用原 Join 的迟到旁路。
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env,
                        bounded ? "hotnews-role-b-verify" : "hotnews-role-b", bounded, bounded);

        //TODO 3.去重、按文章累计，再按十分钟窗口合并分类排名和补算。
        SingleOutputStreamOperator<JSONObject> result = buildRule(RoleStreamUtil.prepare(joined));
        result.print("ROLE_B");
        result.getSideOutput(lateTag).print("ROLE_B_LATE");
        env.execute("Achieve_roleB");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        RuleStreams streams = buildWithLate(joined);
        streams.expiredInputs.print("ROLE_B_LATE_INPUT");
        return streams.ranking;
    }

    /** 一体化 Sink 作业同时取得排名和第一阶段无法在线补算的原始行为。 */
    public static RuleStreams buildWithLate(DataStream<JSONObject> joined) {
        SingleOutputStreamOperator<ArticleScore> articles = joined
                .keyBy(value -> value.getString("category") + "|" + value.getString("article_id"))
                .process(new CountActions());
        return new RuleStreams(articles.keyBy(value -> value.windowStart).process(new RankCategories()),
                articles.getSideOutput(inputLateTag));
    }

    /** 用法：Sink 作业从 ranking 读完整榜单，从 expiredInputs 读超期行为；
     * 思路：第一阶段旁路属于文章得分算子，需与第二阶段排名一起暴露给调用方。 */
    public static class RuleStreams {
        public final SingleOutputStreamOperator<JSONObject> ranking;
        public final DataStream<JSONObject> expiredInputs;

        private RuleStreams(SingleOutputStreamOperator<JSONObject> ranking,
                            DataStream<JSONObject> expiredInputs) {
            this.ranking = ranking;
            this.expiredInputs = expiredInputs;
        }
    }

    /** 单篇文章在一个十分钟窗口内的最新累计值，迟到修正使用更大的 count 覆盖旧值。 */
    public static class ArticleScore {
        public long windowStart;
        public String articleId;
        public String category;
        public String title;
        public long count;
        public int version;
        public long latestTime;
    }

    /** 第一阶段：每个文章键分开计数，超过窗口终点的事件更新原累计值并发送补算。 */
    private static class CountActions extends KeyedProcessFunction<String, JSONObject, ArticleScore> {
        private transient MapState<Long, ArticleScore> windows;
        private transient MapState<Long, Long> corrections;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<Long, ArticleScore> descriptor =
                    new MapStateDescriptor<Long, ArticleScore>(
                            "role-b-article-windows", Long.class, ArticleScore.class);
            windows = getRuntimeContext().getMapState(descriptor);
            corrections = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("role-b-article-corrections", Long.class, Long.class));
        }

        @Override
        public void processElement(JSONObject value, Context context, Collector<ArticleScore> out)
                throws Exception {
            long timestamp = RoleStreamUtil.eventTime(value);
            long start = Math.floorDiv(timestamp, WINDOW_MS) * WINDOW_MS;
            long end = start + WINDOW_MS;
            long watermark = context.timerService().currentWatermark();
            if (watermark >= end + RETENTION_MS) {
                context.output(inputLateTag, value);
                return;
            }
            ArticleScore score = windows.get(start);
            if (score == null) {
                score = new ArticleScore();
                score.windowStart = start;
                score.articleId = value.getString("article_id");
                score.category = value.getString("category");
                if (watermark < end) {
                    context.timerService().registerEventTimeTimer(end);
                }
                context.timerService().registerEventTimeTimer(end + RETENTION_MS + 1);
            }
            score.count++;
            int version = value.getIntValue("article_version");
            if (score.count == 1 || version > score.version
                    || (version == score.version && timestamp >= score.latestTime)) {
                score.version = version;
                score.latestTime = timestamp;
                score.title = value.getString("title");
            }
            windows.put(start, score);
            if (watermark >= end && corrections.get(start) == null) {
                long due = Math.min(end + RETENTION_MS - 1, watermark + CORRECTION_INTERVAL_MS);
                corrections.put(start, due);
                context.timerService().registerEventTimeTimer(due);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<ArticleScore> out)
                throws Exception {
            List<Long> cleanup = new ArrayList<Long>();
            List<Long> emitted = new ArrayList<Long>();
            for (Map.Entry<Long, ArticleScore> entry : windows.entries()) {
                long start = entry.getKey();
                if (timestamp == start + WINDOW_MS) {
                    out.collect(copy(entry.getValue()));
                } else if (timestamp == start + WINDOW_MS + RETENTION_MS + 1) {
                    cleanup.add(start);
                } else if (Long.valueOf(timestamp).equals(corrections.get(start))) {
                    out.collect(copy(entry.getValue()));
                    emitted.add(start);
                }
            }
            for (Long start : emitted) {
                corrections.remove(start);
            }
            for (Long start : cleanup) {
                corrections.remove(start);
                windows.remove(start);
            }
        }

        private ArticleScore copy(ArticleScore source) {
            ArticleScore result = new ArticleScore();
            result.windowStart = source.windowStart;
            result.articleId = source.articleId;
            result.category = source.category;
            result.title = source.title;
            result.count = source.count;
            result.version = source.version;
            result.latestTime = source.latestTime;
            return result;
        }
    }

    /** 第二阶段：以窗口为键保存各文章的最新累计值；修正时输出整份可覆盖的排名。 */
    private static class RankCategories extends KeyedProcessFunction<Long, ArticleScore, JSONObject> {
        private transient MapState<String, ArticleScore> articles;
        private transient ValueState<Boolean> fired;
        private transient ValueState<Long> correctionDue;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, ArticleScore> descriptor =
                    new MapStateDescriptor<String, ArticleScore>(
                            "role-b-ranked-articles", String.class, ArticleScore.class);
            articles = getRuntimeContext().getMapState(descriptor);
            fired = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("role-b-window-fired", Boolean.class));
            correctionDue = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("role-b-rank-correction", Long.class));
        }

        @Override
        public void processElement(ArticleScore score, Context context, Collector<JSONObject> out)
                throws Exception {
            long end = score.windowStart + WINDOW_MS;
            long watermark = context.timerService().currentWatermark();
            if (watermark >= end + RETENTION_MS) {
                context.output(lateTag, score);
                return;
            }
            String key = score.category + "|" + score.articleId;
            ArticleScore previous = articles.get(key);
            if (previous != null && score.count < previous.count) {
                return;
            }
            if (previous != null && score.count == previous.count
                    && score.version <= previous.version) {
                return;
            }
            articles.put(key, score);
            if (fired.value() == null) {
                if (watermark < end) {
                    context.timerService().registerEventTimeTimer(end);
                }
                context.timerService().registerEventTimeTimer(end + RETENTION_MS);
            }
            if (watermark >= end && correctionDue.value() == null) {
                long due = Math.min(end + RETENTION_MS - 1, watermark + CORRECTION_INTERVAL_MS);
                correctionDue.update(due);
                context.timerService().registerEventTimeTimer(due);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<JSONObject> out)
                throws Exception {
            long end = context.getCurrentKey() + WINDOW_MS;
            if (timestamp == end) {
                emitRanking(context.getCurrentKey(), out);
                fired.update(true);
            } else if (Long.valueOf(timestamp).equals(correctionDue.value())) {
                emitRanking(context.getCurrentKey(), out);
                fired.update(true);
                correctionDue.clear();
            } else if (timestamp == end + RETENTION_MS) {
                articles.clear();
                fired.clear();
                correctionDue.clear();
            }
        }

        private void emitRanking(long start, Collector<JSONObject> out) throws Exception {
            Map<String, Map<String, ArticleScore>> grouped = new HashMap<String, Map<String, ArticleScore>>();
            for (ArticleScore score : articles.values()) {
                grouped.computeIfAbsent(score.category, key -> new HashMap<String, ArticleScore>())
                        .put(score.articleId, score);
            }
            List<String> categories = new ArrayList<String>(grouped.keySet());
            categories.sort(Comparator.<String>comparingLong(category -> -total(grouped.get(category)))
                    .thenComparing(category -> category));
            long revision = 0;
            for (Map<String, ArticleScore> scores : grouped.values()) {
                revision += total(scores);
            }
            List<JSONObject> results = new ArrayList<JSONObject>();
            JSONArray snapshot = new JSONArray();
            for (int rank = 0; rank < Math.min(5, categories.size()); rank++) {
                String category = categories.get(rank);
                List<ArticleScore> ranked = new ArrayList<ArticleScore>(grouped.get(category).values());
                ranked.sort(Comparator.<ArticleScore>comparingLong(score -> -score.count)
                        .thenComparing(score -> score.articleId));
                JSONArray topArticles = new JSONArray();
                for (int i = 0; i < Math.min(5, ranked.size()); i++) {
                    ArticleScore score = ranked.get(i);
                    JSONObject item = new JSONObject();
                    item.put("article_id", score.articleId);
                    item.put("title", score.title);
                    item.put("score", score.count);
                    topArticles.add(item);
                }
                JSONObject result = new JSONObject();
                result.put("rank", rank + 1);
                result.put("category", category);
                result.put("score", total(grouped.get(category)));
                result.put("revision", revision);
                result.put("top_articles", topArticles);
                result.put("window_start", Instant.ofEpochMilli(start).toString());
                result.put("window_end", Instant.ofEpochMilli(start + WINDOW_MS).toString());
                result.put("detect_time", Instant.now().toString());
                JSONObject rankSnapshot = new JSONObject();
                rankSnapshot.putAll(result);
                snapshot.add(rankSnapshot);
                results.add(result);
            }
            if (!results.isEmpty()) {
                results.get(0).put("ranking", snapshot);
            }
            for (JSONObject result : results) {
                out.collect(result);
            }
        }

        private long total(Map<String, ArticleScore> values) {
            long sum = 0;
            for (ArticleScore value : values.values()) {
                sum += value.count;
            }
            return sum;
        }
    }
}

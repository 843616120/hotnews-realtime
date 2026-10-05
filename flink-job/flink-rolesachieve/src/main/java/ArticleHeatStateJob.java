import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * 第四天独立的文章热度状态作业：原始模式沿用规则 A 的窗口基线，优化模式按事件 ID
 * 将热点文章分散到 16 个分片，窗口内局部计数，最后按文章合并。两个模式的已告警文章
 * 都另外保存最近一次热度结果，处理时间 TTL 为 24 小时，不改变第三天规则 A/B/C。
 */
public class ArticleHeatStateJob {
    private static final int SHARDS = 16;
    private static final long LATENESS_MS = 30_000L;
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("article-heat-late") {};

    public static void main(String[] args) throws Exception {
        Set<String> options = new HashSet<String>();
        String backend = "hashmap";
        for (String arg : args) {
            if ("--backend=hashmap".equals(arg) || "--backend=rocksdb".equals(arg)) {
                backend = arg.substring("--backend=".length());
                continue;
            }
            if (!("--bounded".equals(arg) || "--optimized".equals(arg)) || !options.add(arg)) {
                throw new IllegalArgumentException(
                        "仅支持 --bounded、--optimized、--backend=hashmap|rocksdb");
            }
        }
        boolean bounded = options.contains("--bounded");
        boolean optimized = options.contains("--optimized");

        //TODO 1.准备环境与检查点
        Configuration conf = new Configuration();
        conf.setString("state.backend.type", backend);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        //TODO 2.复用 Join 和按事件 ID 去重，不修改原有规则作业。
        String group = "hotnews-day4-heat-" + (optimized ? "salted" : "baseline")
                + (bounded ? "-verify" : "");
        DataStream<JSONObject> joined = RoleStreamUtil.prepare(
                ArticleJoinBehavior.createJoinedStream(env, group, bounded));

        //TODO 3.对照原始文章键与两阶段加盐；只输出超过 1000 次的窗口。
        buildRule(joined, optimized).print("ARTICLE_HEAT");
        env.execute("ArticleHeatStateJob");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(
            DataStream<JSONObject> joined, boolean optimized) {
        if (!optimized) {
            return rememberHeat(Achieve_roleA.buildRule(joined));
        }
        SingleOutputStreamOperator<PartialClicks> partial = joined
                .filter(value -> "click".equals(value.getString("action")))
                .keyBy(value -> value.getString("article_id") + "#"
                        + Math.floorMod(value.getString("event_id").hashCode(), SHARDS))
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(lateTag)
                .aggregate(new CountClicks(), new ShardWindow());
        partial.getSideOutput(lateTag).print("ARTICLE_HEAT_LATE");
        return rememberHeat(partial.keyBy(value -> value.articleId).process(new MergeShards()));
    }

    private static SingleOutputStreamOperator<JSONObject> rememberHeat(DataStream<JSONObject> results) {
        return results.keyBy(value -> value.getString("article_id")).process(new RememberHeat());
    }

    /** 分片窗口的点击数和最新文章维度，用于两阶段无损合并。 */
    public static class ClickAccumulator {
        public long count;
        public int version;
        public long latestTime;
        public String title;
        public String category;
    }

    /** 一个文章分片的窗口累计结果；后到的同一分片结果覆盖旧累计值。 */
    public static class PartialClicks {
        public String articleId;
        public int shard;
        public long windowStart;
        public long windowEnd;
        public ClickAccumulator clicks;
    }

    private static class CountClicks
            implements AggregateFunction<JSONObject, ClickAccumulator, ClickAccumulator> {
        @Override
        public ClickAccumulator createAccumulator() {
            return new ClickAccumulator();
        }

        @Override
        public ClickAccumulator add(JSONObject value, ClickAccumulator accumulator) {
            accumulator.count++;
            int version = value.getIntValue("article_version");
            long timestamp = RoleStreamUtil.eventTime(value);
            if (accumulator.count == 1 || version > accumulator.version
                    || (version == accumulator.version && timestamp >= accumulator.latestTime)) {
                accumulator.version = version;
                accumulator.latestTime = timestamp;
                accumulator.title = value.getString("title");
                accumulator.category = value.getString("category");
            }
            return accumulator;
        }

        @Override
        public ClickAccumulator getResult(ClickAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public ClickAccumulator merge(ClickAccumulator left, ClickAccumulator right) {
            if (right.version > left.version
                    || (right.version == left.version && right.latestTime >= left.latestTime)) {
                left.version = right.version;
                left.latestTime = right.latestTime;
                left.title = right.title;
                left.category = right.category;
            }
            left.count += right.count;
            return left;
        }
    }

    private static class ShardWindow
            extends ProcessWindowFunction<ClickAccumulator, PartialClicks, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<ClickAccumulator> values,
                            Collector<PartialClicks> out) {
            PartialClicks partial = new PartialClicks();
            int separator = key.lastIndexOf('#');
            partial.articleId = key.substring(0, separator);
            partial.shard = Integer.parseInt(key.substring(separator + 1));
            partial.windowStart = context.window().getStart();
            partial.windowEnd = context.window().getEnd();
            partial.clicks = values.iterator().next();
            out.collect(partial);
        }
    }

    private static class MergeShards extends KeyedProcessFunction<String, PartialClicks, JSONObject> {
        private transient MapState<String, ClickAccumulator> shards;

        @Override
        public void open(Configuration parameters) {
            shards = getRuntimeContext().getMapState(new MapStateDescriptor<String, ClickAccumulator>(
                    "day4-article-shards", String.class, ClickAccumulator.class));
        }

        @Override
        public void processElement(PartialClicks value, Context context, Collector<JSONObject> out)
                throws Exception {
            shards.put(value.windowStart + ":" + value.shard, value.clicks);
            // 上游窗口结束+30秒清理后，再按文章合并全部分片的最终累计值。
            context.timerService().registerEventTimeTimer(value.windowEnd + LATENESS_MS);
        }

        @Override
        public void onTimer(long timer, OnTimerContext context, Collector<JSONObject> out)
                throws Exception {
            long start = timer - LATENESS_MS - 300_000L;
            ClickAccumulator total = new ClickAccumulator();
            for (int shard = 0; shard < SHARDS; shard++) {
                String key = start + ":" + shard;
                ClickAccumulator part = shards.get(key);
                if (part != null) {
                    new CountClicks().merge(total, part);
                    shards.remove(key);
                }
            }
            if (total.count > 1000) {
                JSONObject result = new JSONObject();
                result.put("article_id", context.getCurrentKey());
                result.put("title", total.title);
                result.put("category", total.category);
                result.put("click_count", total.count);
                result.put("window_start", Instant.ofEpochMilli(start).toString());
                result.put("window_end", Instant.ofEpochMilli(start + 300_000L).toString());
                result.put("detect_time", Instant.now().toString());
                out.collect(result);
            }
        }
    }

    /** 与窗口计数分离的最新热度 ValueState；无更新 24 小时后读取时过期。 */
    private static class RememberHeat extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient ValueState<JSONObject> latestHeat;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<JSONObject> descriptor =
                    new ValueStateDescriptor<JSONObject>("day4-article-latest-heat", JSONObject.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(
                    org.apache.flink.api.common.time.Time.hours(24)).build());
            latestHeat = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(JSONObject value, Context context, Collector<JSONObject> out)
                throws Exception {
            JSONObject previous = latestHeat.value();
            if (previous == null
                    || value.getString("window_end").compareTo(previous.getString("window_end")) > 0
                    || (value.getString("window_end").equals(previous.getString("window_end"))
                    && value.getLongValue("click_count") >= previous.getLongValue("click_count"))) {
                latestHeat.update(value);
            }
            out.collect(value);
        }
    }
}

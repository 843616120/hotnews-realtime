import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

/**
 * 规则 A：识别五分钟内点击量超过 1000 次的热点文章。
 *
 * <p>思路：复用双流 Join 富化结果，先按事件 ID 去重并恢复行为事件时间；
 * 只保留点击，按文章 ID 进入五分钟窗口、每分钟滑动一次，增量计数后输出窗口边界和文章信息。
 * 当前窗口聚合没有单独配置文章热度状态的 24 小时 TTL。</p>
 */
public class Achieve_roleA {
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("role-a-late") {};

    public static void main(String[] args) throws Exception {
        boolean bounded = args.length == 1 && "--bounded".equals(args[0]);
        if (args.length > 0 && !bounded) {
            throw new IllegalArgumentException("仅支持 --bounded 验收参数");
        }
        //TODO 1.准备环境和检查点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(3);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        //TODO 2.接入 Join 富化后的行为流，验收模式使用独立消费组。
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env,
                        bounded ? "hotnews-role-a-verify" : "hotnews-role-a", bounded);

        //TODO 3.去重并恢复事件时间，再统计五分钟滑动窗口内的点击数。
        SingleOutputStreamOperator<JSONObject> result = buildRule(RoleStreamUtil.prepare(joined));
        result.print("ROLE_A");
        result.getSideOutput(lateTag).print("ROLE_A_LATE");
        env.execute("Achieve_roleA");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        // 水位线已经覆盖固定样本的事件时间乱序；窗口关闭后额外保留100秒接收迟到数据。
        return joined.filter(value -> "click".equals(value.getString("action")))
                .keyBy(value -> value.getString("article_id"))
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .allowedLateness(Time.seconds(100))
                .sideOutputLateData(lateTag)
                .aggregate(new CountClicks(), new HotArticleWindow());
    }

    public static class ClickAccumulator {
        public long count;
        public int version;
        public long latestTime;
        public String title;
        public String category;
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

    private static class HotArticleWindow
            extends ProcessWindowFunction<ClickAccumulator, JSONObject, String, TimeWindow> {
        @Override
        public void process(String articleId, Context context, Iterable<ClickAccumulator> values,
                            Collector<JSONObject> out) {
            // 严格大于 1000 次才告警，输出使用事件时间窗口而非作业运行时刻。
            ClickAccumulator count = values.iterator().next();
            if (count.count <= 1000) {
                return;
            }
            JSONObject result = new JSONObject();
            result.put("article_id", articleId);
            result.put("title", count.title);
            result.put("category", count.category);
            result.put("click_count", count.count);
            result.put("window_start", Instant.ofEpochMilli(context.window().getStart()).toString());
            result.put("window_end", Instant.ofEpochMilli(context.window().getEnd()).toString());
            result.put("detect_time", Instant.now().toString());
            out.collect(result);
        }
    }
}

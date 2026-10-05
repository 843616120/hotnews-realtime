import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import java.util.List;
import java.util.Map;

/**
 * 规则 A：识别五分钟内点击量超过 1000 次的热点文章。
 *
 * <p>思路：复用双流 Join 富化结果，先按事件 ID 去重并恢复行为事件时间；
 * 只保留点击，按文章 ID 累计五分钟窗口、每分钟滑动一次；关窗后点击继续更新原累计值，
 * 超过 1000 次时发布完整的修正结果。窗口结束 24 小时后由事件时间定时器清理。</p>
 */
public class Achieve_roleA {
    private static final long MINUTE_MS = 60_000L;
    private static final long WINDOW_MS = 300_000L;
    private static final long RETENTION_MS = 86_400_000L;
    private static final long CORRECTION_INTERVAL_MS = 5_000L;
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("role-a-late") {};

    /** 共用作业可订阅超过规则 A 状态保留时间的事件，供离线补算。 */
    public static OutputTag<JSONObject> lateTag() {
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

        //TODO 2.有界验收从两个 Topic 开头读取，等待文章首次发布事件完成 Join。
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env,
                        bounded ? "hotnews-role-a-verify" : "hotnews-role-a", bounded, bounded);

        //TODO 3.去重并恢复事件时间，再统计五分钟滑动窗口内的点击数。
        SingleOutputStreamOperator<JSONObject> result = buildRule(RoleStreamUtil.prepare(joined));
        result.print("ROLE_A");
        result.getSideOutput(lateTag).print("ROLE_A_LATE");
        env.execute("Achieve_roleA");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        return joined.filter(value -> "click".equals(value.getString("action")))
                .keyBy(value -> value.getString("article_id"))
                .process(new CountClicks());
    }

    /** 一篇文章在一个五分钟窗口的点击累计值和最新文章版本。 */
    public static class ClickAccumulator {
        public long count;
        public int version;
        public long latestTime;
        public String title;
        public String category;
    }

    /** 按文章保存五个重叠窗口；晚到点击合并后重发完整计数，不丢弃已关窗结果。 */
    private static class CountClicks extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient MapState<Long, ClickAccumulator> windows;
        private transient MapState<Long, Long> corrections;

        @Override
        public void open(Configuration parameters) {
            windows = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ClickAccumulator>(
                    "role-a-click-windows", Long.class, ClickAccumulator.class));
            corrections = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>(
                    "role-a-click-corrections", Long.class, Long.class));
        }

        @Override
        public void processElement(JSONObject value, Context context, Collector<JSONObject> out)
                throws Exception {
            long timestamp = RoleStreamUtil.eventTime(value);
            long watermark = context.timerService().currentWatermark();
            long minute = Math.floorDiv(timestamp, MINUTE_MS) * MINUTE_MS;
            boolean expired = false;
            for (int offset = 0; offset < 5; offset++) {
                long start = minute - offset * MINUTE_MS;
                long end = start + WINDOW_MS;
                if (watermark >= end + RETENTION_MS) {
                    expired = true;
                    continue;
                }
                ClickAccumulator count = windows.get(start);
                if (count == null) {
                    count = new ClickAccumulator();
                    if (watermark < end) {
                        context.timerService().registerEventTimeTimer(end);
                    }
                    context.timerService().registerEventTimeTimer(end + RETENTION_MS + 1);
                }
                count.count++;
                int version = value.getIntValue("article_version");
                if (count.count == 1 || version > count.version
                        || (version == count.version && timestamp >= count.latestTime)) {
                    count.version = version;
                    count.latestTime = timestamp;
                    count.title = value.getString("title");
                    count.category = value.getString("category");
                }
                windows.put(start, count);
                if (watermark >= end && corrections.get(start) == null) {
                    long due = Math.min(end + RETENTION_MS - 1, watermark + CORRECTION_INTERVAL_MS);
                    corrections.put(start, due);
                    context.timerService().registerEventTimeTimer(due);
                }
            }
            if (expired) {
                context.output(lateTag, value);
            }
        }

        @Override
        public void onTimer(long timer, OnTimerContext context, Collector<JSONObject> out)
                throws Exception {
            List<Long> cleanup = new ArrayList<Long>();
            List<Long> emitted = new ArrayList<Long>();
            for (Map.Entry<Long, ClickAccumulator> entry : windows.entries()) {
                long start = entry.getKey();
                if (timer == start + WINDOW_MS) {
                    emitHot(context.getCurrentKey(), start, entry.getValue(), out);
                } else if (timer == start + WINDOW_MS + RETENTION_MS + 1) {
                    cleanup.add(start);
                } else if (Long.valueOf(timer).equals(corrections.get(start))) {
                    emitHot(context.getCurrentKey(), start, entry.getValue(), out);
                    emitted.add(start);
                }
            }
            for (Long start : emitted) {
                corrections.remove(start);
            }
            for (Long start : cleanup) {
                windows.remove(start);
                corrections.remove(start);
            }
        }

        private void emitHot(String articleId, long start, ClickAccumulator count,
                             Collector<JSONObject> out) {
            if (count.count <= 1000) {
                return;
            }
            JSONObject result = new JSONObject();
            result.put("article_id", articleId);
            result.put("title", count.title);
            result.put("category", count.category);
            result.put("click_count", count.count);
            result.put("window_start", Instant.ofEpochMilli(start).toString());
            result.put("window_end", Instant.ofEpochMilli(start + WINDOW_MS).toString());
            result.put("detect_time", Instant.now().toString());
            out.collect(result);
        }
    }
}

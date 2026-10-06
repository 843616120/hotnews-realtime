import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 第四天独立的 IP 状态与倾斜实验：一分钟滚动（不重叠）窗口按 IP 统计不同文章、
 * 点击次数、阅读时长，并保存窗口起止；原始模式一片，优化模式按 event_id 分为
 * 16 片做局部状态计算，再按 IP 合并。两条路径使用相同事件时间、TTL 与告警阈值。
 * 与规则 C 的“每次点击回看一分钟”不是同一种窗口语义，不能直接替换其结果。
 */
public class IpWindowStateJob {
    private static final int SHARDS = 16;
    private static final long MINUTE_MS = 60_000L;
    private static final long LATENESS_MS = 30_000L;
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("day4-ip-late") {};

    public static void main(String[] args) throws Exception {
        Set<String> options = new HashSet<String>();
        String backend = "hashmap";
        String runId = "";
        for (String arg : args) {
            if ("--backend=hashmap".equals(arg) || "--backend=rocksdb".equals(arg)) {
                backend = arg.substring("--backend=".length());
                continue;
            }
            if (arg.startsWith("--run-id=") && arg.length() > "--run-id=".length()) {
                runId = arg.substring("--run-id=".length());
                continue;
            }
            if (!("--bounded".equals(arg) || "--optimized".equals(arg)) || !options.add(arg)) {
                throw new IllegalArgumentException(
                        "仅支持 --bounded、--optimized、--backend=hashmap|rocksdb、--run-id=...");
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

        //TODO 2.通过独立消费组接入 Join，按 event_id 去重并恢复行为事件时间。
        String group = "hotnews-day4-ip-" + (optimized ? "salted" : "baseline")
                + (bounded ? "-verify" : "")
                + (runId.isEmpty() ? "" : "-" + runId);
        DataStream<JSONObject> joined = RoleStreamUtil.prepare(
                ArticleJoinBehavior.createJoinedStream(env, group, bounded));

        //TODO 3.分别以一片和十六片执行相同 IP 窗口计算，输出告警及迟到旁路。
        buildRule(joined, optimized).print("IP_STATE");
        env.execute("IpWindowStateJob");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(
            DataStream<JSONObject> joined, boolean optimized) {
        final int shardCount = optimized ? SHARDS : 1;
        SingleOutputStreamOperator<IpPartial> partial = joined
                .filter(value -> "click".equals(value.getString("action")))
                .keyBy(value -> value.getString("ip") + "|"
                        + Math.floorDiv(RoleStreamUtil.eventTime(value), MINUTE_MS) * MINUTE_MS
                        + "#" + Math.floorMod(value.getString("event_id").hashCode(), shardCount))
                .process(new CountIpShard());
        partial.getSideOutput(lateTag).print("IP_STATE_LATE");
        return partial.keyBy(value -> value.ip).process(new MergeIpShards(shardCount));
    }

    /** 每个 IP 分片当前窗口的明细状态；聚合后不把整份点击 JSON 送到第二阶段。 */
    public static class IpWindowState {
        public String ip;
        public HashSet<String> articleIds = new HashSet<String>();
        public long clickCount;
        public long durationSum;
        public long windowStart;
        public long windowEnd;
    }

    /** 一个 IP 在某个事件时间窗口、某个分片的部分结果。 */
    public static class IpPartial {
        public String ip;
        public int shard;
        public IpWindowState state;
    }

    private static class CountIpShard extends KeyedProcessFunction<String, JSONObject, IpPartial> {
        private transient ValueState<IpWindowState> windowState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<IpWindowState> descriptor =
                    new ValueStateDescriptor<IpWindowState>("day4-ip-window", IpWindowState.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
            windowState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(JSONObject click, Context context, Collector<IpPartial> out)
                throws Exception {
            long timestamp = RoleStreamUtil.eventTime(click);
            long start = Math.floorDiv(timestamp, MINUTE_MS) * MINUTE_MS;
            long watermark = context.timerService().currentWatermark();
            if (watermark != Long.MIN_VALUE && start + MINUTE_MS + LATENESS_MS - 1 <= watermark) {
                context.output(lateTag, click);
                return;
            }
            IpWindowState state = windowState.value();
            if (state == null) {
                state = new IpWindowState();
                state.ip = click.getString("ip");
                state.windowStart = start;
                state.windowEnd = start + MINUTE_MS;
            }
            state.articleIds.add(click.getString("article_id"));
            state.clickCount++;
            state.durationSum += click.getLongValue("read_duration_ms");
            windowState.update(state);
            context.timerService().registerEventTimeTimer(state.windowEnd + LATENESS_MS - 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<IpPartial> out)
                throws Exception {
            IpWindowState state = windowState.value();
            if (state == null) {
                return;
            }
            IpPartial partial = new IpPartial();
            partial.ip = state.ip;
            partial.shard = Integer.parseInt(context.getCurrentKey()
                    .substring(context.getCurrentKey().lastIndexOf('#') + 1));
            partial.state = state;
            out.collect(partial);
            windowState.clear();
        }
    }

    private static class MergeIpShards extends KeyedProcessFunction<String, IpPartial, JSONObject> {
        private final int shardCount;
        private transient MapState<String, IpWindowState> partials;
        private transient ValueState<IpWindowState> latestWindow;
        private transient ValueState<Long> lastAlertWindow;

        private MergeIpShards(int shardCount) {
            this.shardCount = shardCount;
        }

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(1)).build();
            MapStateDescriptor<String, IpWindowState> partDescriptor =
                    new MapStateDescriptor<String, IpWindowState>(
                            "day4-ip-partials", String.class, IpWindowState.class);
            partDescriptor.enableTimeToLive(ttl);
            partials = getRuntimeContext().getMapState(partDescriptor);
            ValueStateDescriptor<IpWindowState> latestDescriptor =
                    new ValueStateDescriptor<IpWindowState>("day4-ip-latest-window", IpWindowState.class);
            latestDescriptor.enableTimeToLive(ttl);
            latestWindow = getRuntimeContext().getState(latestDescriptor);
            ValueStateDescriptor<Long> alertDescriptor =
                    new ValueStateDescriptor<Long>("day4-ip-last-alert", Long.class);
            alertDescriptor.enableTimeToLive(ttl);
            lastAlertWindow = getRuntimeContext().getState(alertDescriptor);
        }

        @Override
        public void processElement(IpPartial value, Context context, Collector<JSONObject> out)
                throws Exception {
            partials.put(value.state.windowStart + ":" + value.shard, value.state);
            // 第一阶段的所有分片在窗口+30秒结算，第二阶段再等 1ms 合并。
            context.timerService().registerEventTimeTimer(value.state.windowEnd + LATENESS_MS + 1);
        }

        @Override
        public void onTimer(long timer, OnTimerContext context, Collector<JSONObject> out)
                throws Exception {
            long start = timer - LATENESS_MS - 1 - MINUTE_MS;
            IpWindowState total = new IpWindowState();
            total.ip = context.getCurrentKey();
            total.windowStart = start;
            total.windowEnd = start + MINUTE_MS;
            for (int shard = 0; shard < shardCount; shard++) {
                String key = start + ":" + shard;
                IpWindowState part = partials.get(key);
                if (part != null) {
                    total.articleIds.addAll(part.articleIds);
                    total.clickCount += part.clickCount;
                    total.durationSum += part.durationSum;
                    partials.remove(key);
                }
            }
            if (total.clickCount == 0) {
                return;
            }
            latestWindow.update(total);
            Long lastAlert = lastAlertWindow.value();
            if (total.articleIds.size() <= 50 || total.durationSum >= 2000L * total.clickCount
                    || (lastAlert != null && lastAlert == start)) {
                return;
            }
            lastAlertWindow.update(start);
            List<String> ids = new ArrayList<String>(total.articleIds);
            Collections.sort(ids);
            JSONObject result = new JSONObject();
            result.put("ip", total.ip);
            result.put("article_count", ids.size());
            result.put("article_ids", ids);
            result.put("click_count", total.clickCount);
            result.put("avg_read_duration_ms", (double) total.durationSum / total.clickCount);
            result.put("window_start", Instant.ofEpochMilli(total.windowStart).toString());
            result.put("window_end", Instant.ofEpochMilli(total.windowEnd).toString());
            result.put("detect_time", Instant.now().toString());
            out.collect(result);
        }
    }
}

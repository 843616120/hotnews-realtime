import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 从 Achieve_roleA 独立拷贝的第六天压测作业；原规则 A 的文件与状态均不修改。
 * 思路：隔离限速 Source 经过规则 A 的五分钟滑窗，同时分支进入可降速的
 * 明细 Sink 探针。通过 REST 采集吞吐、反压及 Checkpoint，与不同速率和
 * Sink 并行度的运行比较。探针只模拟外部写入耗时，不宣称写入真实数据库。
 */
public class Day6BackpressureJob {
    private static final long MINUTE_MS = 60_000L;
    private static final long WINDOW_MS = 300_000L;
    private static final long RETENTION_MS = 86_400_000L;
    private static final long CORRECTION_INTERVAL_MS = 5_000L;
    private static final OutputTag<JSONObject> lateTag = new OutputTag<JSONObject>("day6-role-a-late") {};

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(options.parallelism);
        env.enableCheckpointing(5_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setCheckpointTimeout(60_000L);
        checkpoints.setMaxConcurrentCheckpoints(1);
        String storage = System.getenv("HOTNEWS_DAY6_CHECKPOINT_DIR");
        if (storage != null && !storage.isEmpty()) checkpoints.setCheckpointStorage(storage);

        DataStream<JSONObject> input;
        if (options.kafkaTopic != null) {
            KafkaSource<JSONObject> source = KafkaSource.<JSONObject>builder()
                    .setBootstrapServers(System.getenv().getOrDefault(
                            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                    .setTopics(options.kafkaTopic)
                    .setGroupId(options.kafkaGroup)
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                    .setProperty("commit.offsets.on.checkpoint", "true")
                    .setProperty("enable.auto.commit", "false")
                    .setDeserializer(new Day6KafkaRecordDeserializer(options.keys))
                    .build();
            input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Day6 isolated Kafka source")
                    .setParallelism(options.parallelism);
        } else {
            input = env.addSource(new Day6SyntheticSource(options.rate, options.seconds, options.keys),
                    "Day6 isolated source").setParallelism(options.parallelism);
        }
        SingleOutputStreamOperator<JSONObject> clicks = input
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                                Duration.ofSeconds(2))
                        .withTimestampAssigner((value, previous) -> Instant.parse(
                                value.getString("event_time")).toEpochMilli()));
        clicks.addSink(new Day6DelaySink(options.delayMs))
                .name("Day6 simulated " + options.target + " sink")
                .disableChaining()
                .setParallelism(options.sinkParallelism);
        buildRule(clicks).addSink(new Day6DelaySink(0))
                .name("Day6 rule A result probe").setParallelism(1);
        if (options.plan) System.out.println(env.getExecutionPlan());
        else env.execute("Day6BackpressureJob");
    }

    /** 沿用规则 A 的按文章键统计语义，确保受压支路运行的是实际窗口计算。 */
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

    /** 从规则 A 复制的滑窗状态；迟到修正与到期清理保持相同的业务行为。 */
    private static class CountClicks extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient MapState<Long, ClickAccumulator> windows;
        private transient MapState<Long, Long> corrections;

        @Override
        public void open(Configuration parameters) {
            windows = getRuntimeContext().getMapState(new MapStateDescriptor<Long, ClickAccumulator>(
                    "day6-role-a-click-windows", Long.class, ClickAccumulator.class));
            corrections = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>(
                    "day6-role-a-click-corrections", Long.class, Long.class));
        }

        @Override
        public void processElement(JSONObject value, Context context, Collector<JSONObject> out)
                throws Exception {
            long timestamp = Instant.parse(value.getString("event_time")).toEpochMilli();
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
                    if (watermark < end) context.timerService().registerEventTimeTimer(end);
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
            if (expired) context.output(lateTag, value);
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
            for (Long start : emitted) corrections.remove(start);
            for (Long start : cleanup) {
                windows.remove(start);
                corrections.remove(start);
            }
        }

        private void emitHot(String articleId, long start, ClickAccumulator count,
                             Collector<JSONObject> out) {
            if (count.count <= 1000) return;
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

    /** 校验命令行范围，避免误提交不受控的高负载作业。 */
    static class Options {
        int rate = 2000;
        int seconds = 15;
        int parallelism = 2;
        int sinkParallelism = 2;
        int delayMs = 0;
        int keys = 200;
        String target = "mysql";
        String kafkaTopic;
        String kafkaGroup;
        boolean plan;

        static Options parse(String[] args) {
            Options options = new Options();
            for (int i = 0; i < args.length; i++) {
                if ("--plan".equals(args[i])) {
                    options.plan = true;
                    continue;
                }
                if (i + 1 >= args.length) throw new IllegalArgumentException("参数缺少值: " + args[i]);
                String value = args[++i];
                switch (args[i - 1]) {
                    case "--rate": options.rate = Integer.parseInt(value); break;
                    case "--seconds": options.seconds = Integer.parseInt(value); break;
                    case "--parallelism": options.parallelism = Integer.parseInt(value); break;
                    case "--sink-parallelism": options.sinkParallelism = Integer.parseInt(value); break;
                    case "--delay-ms": options.delayMs = Integer.parseInt(value); break;
                    case "--keys": options.keys = Integer.parseInt(value); break;
                    case "--target": options.target = value; break;
                    case "--kafka-topic": options.kafkaTopic = value; break;
                    case "--kafka-group": options.kafkaGroup = value; break;
                    default: throw new IllegalArgumentException("未知参数: " + args[i - 1]);
                }
            }
            if (options.rate < 1 || options.rate > 10_000 || options.seconds < 1 || options.seconds > 120
                    || options.parallelism < 1 || options.parallelism > 4
                    || options.sinkParallelism < 1 || options.sinkParallelism > 4
                    || options.keys < 1 || options.keys > 10_000
                    || options.delayMs < 0 || options.delayMs > 1000
                    || !("mysql".equals(options.target) || "redis".equals(options.target))) {
                throw new IllegalArgumentException("参数超出安全范围（rate 1..10000, seconds 1..120,"
                        + " parallelism 1..4, keys 1..10000, delay-ms 0..1000, target mysql|redis）");
            }
            if ((options.kafkaTopic == null) != (options.kafkaGroup == null)
                    || (options.kafkaTopic != null && (!options.kafkaTopic.matches("hotnews-day6-[a-z0-9-]+")
                    || !options.kafkaGroup.matches("hotnews-day6-[a-z0-9-]+")))) {
                throw new IllegalArgumentException("Kafka 模式必须同时指定隔离的 hotnews-day6-* Topic 和消费组");
            }
            return options;
        }
    }
}

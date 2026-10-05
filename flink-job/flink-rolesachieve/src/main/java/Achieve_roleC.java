import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 规则 C：识别一分钟内点击超过 50 篇不同文章、平均阅读不足两秒的 IP。
 *
 * <p>思路：复用 Join 富化结果并只保留点击，按 IP 保存点击明细；每条点击注册事件时间
 * 定时器，触发时回看一分钟，计算不同文章数、点击数和阅读时长之和，同一分钟只告警一次。
 * 目前集合、计数、时长总和和窗口边界均在定时器中临时计算，尚未单独作为 IP 状态保存。</p>
 */
public class Achieve_roleC {
    private static final long MINUTE_MS = 60_000L;
    private static final long LATENESS_MS = 30_000L;
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("role-c-late") {};

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
                        bounded ? "hotnews-role-c-verify" : "hotnews-role-c", bounded);

        //TODO 3.去重并恢复事件时间，按 IP 检查滚动一分钟的不同文章数与平均阅读时长。
        SingleOutputStreamOperator<JSONObject> result = buildRule(RoleStreamUtil.prepare(joined));
        result.print("ROLE_C");
        result.getSideOutput(lateTag).print("ROLE_C_LATE");
        env.execute("Achieve_roleC");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        return joined.filter(value -> "click".equals(value.getString("action")))
                .keyBy(value -> value.getString("ip"))
                .process(new DetectSuspiciousIp());
    }

    private static class DetectSuspiciousIp extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient ListState<JSONObject> clicks;
        private transient ValueState<Long> lastAlertMinute;

        @Override
        public void open(Configuration parameters) {
            // 点击明细与上次告警分钟均设置一小时 TTL；这不是文章热度状态的 TTL。
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(1)).build();
            ListStateDescriptor<JSONObject> clickDescriptor =
                    new ListStateDescriptor<JSONObject>("ip-clicks", JSONObject.class);
            clickDescriptor.enableTimeToLive(ttl);
            clicks = getRuntimeContext().getListState(clickDescriptor);
            ValueStateDescriptor<Long> alertDescriptor =
                    new ValueStateDescriptor<Long>("ip-last-alert-minute", Long.class);
            alertDescriptor.enableTimeToLive(ttl);
            lastAlertMinute = getRuntimeContext().getState(alertDescriptor);
        }

        @Override
        public void processElement(JSONObject click, Context context, Collector<JSONObject> out)
                throws Exception {
            long timestamp = RoleStreamUtil.eventTime(click);
            long watermark = context.timerService().currentWatermark();
            if (watermark != Long.MIN_VALUE && timestamp + LATENESS_MS <= watermark) {
                context.output(lateTag, click);
                return;
            }
            clicks.add(click);
            context.timerService().registerEventTimeTimer(timestamp + LATENESS_MS);
        }

        @Override
        public void onTimer(long timer, OnTimerContext context, Collector<JSONObject> out) throws Exception {
            // 遍历保留的点击明细，临时计算当前窗口指标，并淘汰窗口外的记录。
            long end = timer - LATENESS_MS;
            List<JSONObject> retained = new ArrayList<JSONObject>();
            Set<String> distinctArticles = new HashSet<String>();
            long durationSum = 0;
            int clickCount = 0;
            for (JSONObject click : clicks.get()) {
                long timestamp = RoleStreamUtil.eventTime(click);
                if (timestamp > end - MINUTE_MS) {
                    retained.add(click);
                    if (timestamp <= end) {
                        distinctArticles.add(click.getString("article_id"));
                        durationSum += click.getLongValue("read_duration_ms");
                        clickCount++;
                    }
                }
            }
            clicks.update(retained);
            long minute = end / MINUTE_MS;
            Long lastMinute = lastAlertMinute.value();
            if (distinctArticles.size() <= 50 || clickCount == 0
                    || durationSum >= 2000L * clickCount
                    || (lastMinute != null && lastMinute == minute)) {
                return;
            }
            lastAlertMinute.update(minute);
            JSONObject result = new JSONObject();
            result.put("ip", context.getCurrentKey());
            result.put("article_count", distinctArticles.size());
            result.put("click_count", clickCount);
            result.put("avg_read_duration_ms", (double) durationSum / clickCount);
            result.put("window_start", Instant.ofEpochMilli(end - MINUTE_MS).toString());
            result.put("window_end", Instant.ofEpochMilli(end).toString());
            result.put("detect_time", Instant.now().toString());
            out.collect(result);
        }
    }
}

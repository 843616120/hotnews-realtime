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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则 C：识别一分钟内点击超过 50 篇不同文章、平均阅读不足两秒的 IP。
 *
 * <p>思路：按 IP 保存点击明细，按事件时间复算点击回看的一分钟。后到点击可能
 * 改变该分钟最早告警，也可能使平均时长超过阈值；因此同一分钟按 alert_minute
 * 发布更新或撤销。事件时间超过窗口终点 24 小时的修正进入旁路。</p>
 */
public class Achieve_roleC {
    private static final long MINUTE_MS = 60_000L;
    private static final long RETENTION_MS = 86_400_000L;
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("role-c-late") {};

    /** 共用作业可订阅超过规则 C 状态保留时间的点击。 */
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

        //TODO 2.有界验收回放文章和行为，文章未到时不丢掉有效点击。
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env,
                        bounded ? "hotnews-role-c-verify" : "hotnews-role-c", bounded, bounded);

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

    /** 一分钟中按事件时间最早的有效告警值；用于比较更新和发出撤销。 */
    public static class CandidateAlert {
        public long windowEnd;
        public int articleCount;
        public int clickCount;
        public long durationSum;
    }

    /** 按 IP 保存点击和每分钟的告警版本，晚到点击重新检查本分钟与下一分钟。 */
    private static class DetectSuspiciousIp extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient MapState<String, JSONObject> clicks;
        private transient MapState<Long, CandidateAlert> alerts;

        @Override
        public void open(Configuration parameters) {
            clicks = getRuntimeContext().getMapState(new MapStateDescriptor<String, JSONObject>(
                    "role-c-clicks", String.class, JSONObject.class));
            alerts = getRuntimeContext().getMapState(new MapStateDescriptor<Long, CandidateAlert>(
                    "role-c-alerts", Long.class, CandidateAlert.class));
        }

        @Override
        public void processElement(JSONObject click, Context context, Collector<JSONObject> out)
                throws Exception {
            long timestamp = RoleStreamUtil.eventTime(click);
            long watermark = context.timerService().currentWatermark();
            long minute = Math.floorDiv(timestamp, MINUTE_MS) * MINUTE_MS;
            if (watermark >= minute + 2 * MINUTE_MS + RETENTION_MS) {
                context.output(lateTag, click);
                return;
            }
            clicks.put(click.getString("event_id"), click);
            boolean partiallyExpired = false;
            for (int offset = 0; offset <= 1; offset++) {
                long targetMinute = minute + offset * MINUTE_MS;
                long expires = targetMinute + MINUTE_MS + RETENTION_MS;
                if (watermark >= expires) {
                    partiallyExpired = true;
                    continue;
                }
                context.timerService().registerEventTimeTimer(expires + 1);
                recalculate(targetMinute, context.getCurrentKey(), out);
            }
            if (partiallyExpired) {
                context.output(lateTag, click);
            }
        }

        @Override
        public void onTimer(long timer, OnTimerContext context, Collector<JSONObject> out) throws Exception {
            List<String> expiredClicks = new ArrayList<String>();
            for (Map.Entry<String, JSONObject> entry : clicks.entries()) {
                long minute = Math.floorDiv(RoleStreamUtil.eventTime(entry.getValue()), MINUTE_MS) * MINUTE_MS;
                if (timer >= minute + 2 * MINUTE_MS + RETENTION_MS + 1) {
                    expiredClicks.add(entry.getKey());
                }
            }
            for (String id : expiredClicks) {
                clicks.remove(id);
            }
            alerts.remove(timer - RETENTION_MS - MINUTE_MS - 1);
        }

        private void recalculate(long minute, String ip, Collector<JSONObject> out) throws Exception {
            List<JSONObject> ordered = new ArrayList<JSONObject>();
            for (JSONObject click : clicks.values()) {
                long ts = RoleStreamUtil.eventTime(click);
                if (ts >= minute - MINUTE_MS && ts < minute + MINUTE_MS) {
                    ordered.add(click);
                }
            }
            ordered.sort(Comparator.comparingLong(RoleStreamUtil::eventTime)
                    .thenComparing(value -> value.getString("event_id")));
            ArrayDeque<JSONObject> recent = new ArrayDeque<JSONObject>();
            Map<String, Integer> articles = new HashMap<String, Integer>();
            long durationSum = 0;
            CandidateAlert candidate = null;
            for (int index = 0; index < ordered.size();) {
                long end = RoleStreamUtil.eventTime(ordered.get(index));
                while (!recent.isEmpty() && RoleStreamUtil.eventTime(recent.peekFirst()) <= end - MINUTE_MS) {
                    JSONObject removed = recent.removeFirst();
                    String articleId = removed.getString("article_id");
                    int count = articles.get(articleId);
                    if (count == 1) {
                        articles.remove(articleId);
                    } else {
                        articles.put(articleId, count - 1);
                    }
                    durationSum -= removed.getLongValue("read_duration_ms");
                }
                while (index < ordered.size() && RoleStreamUtil.eventTime(ordered.get(index)) == end) {
                    JSONObject value = ordered.get(index++);
                    recent.addLast(value);
                    String articleId = value.getString("article_id");
                    articles.put(articleId, articles.getOrDefault(articleId, 0) + 1);
                    durationSum += value.getLongValue("read_duration_ms");
                }
                if (end >= minute && articles.size() > 50
                        && durationSum < 2000L * recent.size()) {
                    candidate = new CandidateAlert();
                    candidate.windowEnd = end;
                    candidate.articleCount = articles.size();
                    candidate.clickCount = recent.size();
                    candidate.durationSum = durationSum;
                    break;
                }
            }
            CandidateAlert previous = alerts.get(minute);
            if (candidate == null) {
                if (previous != null) {
                    alerts.remove(minute);
                    JSONObject retraction = new JSONObject();
                    retraction.put("ip", ip);
                    retraction.put("alert_minute", minute);
                    retraction.put("retracted", true);
                    out.collect(retraction);
                }
                return;
            }
            if (previous != null && previous.windowEnd == candidate.windowEnd
                    && previous.articleCount == candidate.articleCount
                    && previous.clickCount == candidate.clickCount
                    && previous.durationSum == candidate.durationSum) {
                return;
            }
            alerts.put(minute, candidate);
            JSONObject result = new JSONObject();
            result.put("ip", ip);
            result.put("alert_minute", minute);
            result.put("article_count", candidate.articleCount);
            result.put("click_count", candidate.clickCount);
            result.put("avg_read_duration_ms", (double) candidate.durationSum / candidate.clickCount);
            result.put("window_start", Instant.ofEpochMilli(candidate.windowEnd - MINUTE_MS).toString());
            result.put("window_end", Instant.ofEpochMilli(candidate.windowEnd).toString());
            result.put("detect_time", Instant.now().toString());
            out.collect(result);
        }
    }
}

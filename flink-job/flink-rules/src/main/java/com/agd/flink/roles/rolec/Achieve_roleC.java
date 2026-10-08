package com.agd.flink.roles.rolec;

import com.agd.flink.join.ArticleJoinBehavior;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import util.FlinkMetricsUtil;
import util.FlinkRuntimeUtil;
import util.FlinkSinkUtil;
import util.RoleStreamUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则 C：同 IP 最近一分钟点击超过 50 篇不同文章，平均阅读时长严格小于 2000ms。
 * 按 IP 保存点击和回看状态；同 IP 每分钟保留最早命中的一次，晚到点击可修正或撤销。
 * IP 状态 TTL 一小时，事件时间在线补算保留 65 分钟；告警及超期数据写 MySQL。
 */
public class Achieve_roleC {
    private static final long MINUTE_MS = 60_000L;
    private static final long ALLOWED_LATENESS_MS = 65 * MINUTE_MS;
    private static final OutputTag<JSONObject> lateTag = new OutputTag<JSONObject>("role-c-late") {};

    public static OutputTag<JSONObject> lateTag() { return lateTag; }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 10013);
        configuration.set(RestOptions.BIND_PORT, "10013");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(3);
        FlinkRuntimeUtil.configureCheckpointing(env, "HOTNEWS_CHECKPOINT_DIR");

        // 调用 Join，沿用行为事件时间和上游 Watermark，不再生成新水位线。
        SingleOutputStreamOperator<JSONObject> clean = RoleStreamUtil.prepare(
                ArticleJoinBehavior.createJoinedStream(env, "hotnews-role-c"));
        FlinkSinkUtil.sinkMySql(clean, RoleStreamUtil.MYSQL_SQL,
                RoleStreamUtil::bindMySql, null, "MySQL clean behavior");
        SingleOutputStreamOperator<JSONObject> result = buildRule(clean);
        DataStream<JSONObject> late = result.getSideOutput(lateTag)
                .map(value -> RoleStreamUtil.audit(value, "ROLE_C_LATE"));
        FlinkSinkUtil.sinkMySql(late, RoleStreamUtil.AUDIT_MYSQL_SQL,
                RoleStreamUtil::bindAudit, null, "MySQL role C late events");
        // 同一告警主键可多次修正；指标和 JDBC Sink 单路写入，避免批次交叉争锁。
        DataStream<JSONObject> measured = FlinkMetricsUtil.measure(result, "role_c_result").setParallelism(1);
        FlinkSinkUtil.sinkMySql(measured, MYSQL_SQL, Achieve_roleC::bindMySql,
                null, "MySQL IP alerts").setParallelism(1);
        result.print("ROLE_C");
        result.getSideOutput(lateTag).print("ROLE_C_LATE");
        env.execute("Achieve_roleC");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        return joined.filter(value -> "click".equals(value.getString("action")))
                .keyBy(value -> value.getString("ip")).process(new DetectIp());
    }

    /** 保存考核要求的文章集合、点击数、阅读时长总和和回看起止时间。 */
    public static class IpWindowState {
        public long start;
        public long end;
        public List<String> articleIds;
        public int clickCount;
        public long durationSum;
        public boolean alert;
        public long revision;
    }

    private static class DetectIp extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient MapState<String, JSONObject> clicks;
        private transient MapState<Long, IpWindowState> windows;

        @Override
        public void open(Configuration parameters) {
            // TTL 按处理时间；事件时间定时器清理补算期，TTL 在水位线停滞时兜底。
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupIncrementally(100, true).build();
            MapStateDescriptor<String, JSONObject> records =
                    new MapStateDescriptor<>("role-c-ip-clicks", String.class, JSONObject.class);
            records.enableTimeToLive(ttl);
            clicks = getRuntimeContext().getMapState(records);
            MapStateDescriptor<Long, IpWindowState> snapshots =
                    new MapStateDescriptor<>("role-c-ip-windows", Long.class, IpWindowState.class);
            snapshots.enableTimeToLive(ttl);
            windows = getRuntimeContext().getMapState(snapshots);
        }

        @Override
        public void processElement(JSONObject click, Context ctx, Collector<JSONObject> out) throws Exception {
            long minute = Math.floorDiv(RoleStreamUtil.eventTime(click), MINUTE_MS) * MINUTE_MS;
            long watermark = ctx.timerService().currentWatermark();
            // 点击影响所在分钟和下一分钟的回看；两者都过期时只留旁路，不更新状态。
            if (watermark >= minute + 2 * MINUTE_MS + ALLOWED_LATENESS_MS) {
                reportLate(click, watermark, ctx);
                return;
            }
            clicks.put(click.getString("event_id"), click);
            boolean expired = false;
            for (int offset = 0; offset <= 1; offset++) {
                long target = minute + offset * MINUTE_MS;
                long cleanup = target + MINUTE_MS + ALLOWED_LATENESS_MS;
                if (watermark >= cleanup) {
                    expired = true;
                } else {
                    ctx.timerService().registerEventTimeTimer(cleanup);
                    recalculate(target, ctx.getCurrentKey(), out);
                }
            }
            // 只过期一个分钟时，另一个分钟仍补算，并留痕说明部分回看无法恢复。
            if (expired) reportLate(click, watermark, ctx);
        }

        private void reportLate(JSONObject click, long watermark, Context ctx) {
            JSONObject late = new JSONObject();
            late.putAll(click);
            late.put("watermark_ms", watermark);
            late.put("dirty_reason", "ROLE_C_LOOKBACK_EXPIRED");
            ctx.output(lateTag, late);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            windows.remove(timestamp - MINUTE_MS - ALLOWED_LATENESS_MS);
            List<String> expired = new ArrayList<>();
            for (Map.Entry<String, JSONObject> record : clicks.entries()) {
                long minute = Math.floorDiv(RoleStreamUtil.eventTime(record.getValue()), MINUTE_MS) * MINUTE_MS;
                if (timestamp >= minute + 2 * MINUTE_MS + ALLOWED_LATENESS_MS) expired.add(record.getKey());
            }
            for (String id : expired) clicks.remove(id);
        }

        private void recalculate(long minute, String ip, Collector<JSONObject> out) throws Exception {
            List<JSONObject> ordered = new ArrayList<>();
            for (JSONObject click : clicks.values()) {
                long time = RoleStreamUtil.eventTime(click);
                if (time > minute - MINUTE_MS && time < minute + MINUTE_MS) ordered.add(click);
            }
            ordered.sort(Comparator.comparingLong(RoleStreamUtil::eventTime)
                    .thenComparing(value -> value.getString("event_id")));
            ArrayDeque<JSONObject> recent = new ArrayDeque<>();
            Map<String, Integer> articles = new HashMap<>();
            long duration = 0;
            IpWindowState current = null;
            for (int i = 0; i < ordered.size();) {
                long end = RoleStreamUtil.eventTime(ordered.get(i));
                // 范围为 (end-60s, end]；一篇文章的剩余点击数归零后才移出文章集合。
                while (!recent.isEmpty() && RoleStreamUtil.eventTime(recent.peekFirst()) <= end - MINUTE_MS) {
                    JSONObject removed = recent.removeFirst();
                    String article = removed.getString("article_id");
                    int count = articles.get(article);
                    if (count == 1) articles.remove(article); else articles.put(article, count - 1);
                    duration -= removed.getLongValue("read_duration_ms");
                }
                // 同毫秒的点击一起计入，避免输入顺序改变告警结果。
                while (i < ordered.size() && RoleStreamUtil.eventTime(ordered.get(i)) == end) {
                    JSONObject click = ordered.get(i++);
                    recent.addLast(click);
                    String article = click.getString("article_id");
                    articles.put(article, articles.getOrDefault(article, 0) + 1);
                    duration += click.getLongValue("read_duration_ms");
                }
                if (end < minute) continue;
                current = new IpWindowState();
                current.start = end - MINUTE_MS;
                current.end = end;
                current.articleIds = new ArrayList<>(articles.keySet());
                Collections.sort(current.articleIds);
                current.clickCount = recent.size();
                current.durationSum = duration;
                current.alert = articles.size() > 50 && duration < 2000L * recent.size();
                // 同一分钟取最早命中的一次；未命中也保存最后一次的回看状态。
                if (current.alert) break;
            }
            if (current == null) return;
            IpWindowState previous = windows.get(minute);
            if (previous != null && previous.end == current.end && previous.clickCount == current.clickCount
                    && previous.durationSum == current.durationSum && previous.alert == current.alert
                    && previous.articleIds.equals(current.articleIds)) return;
            // 撤销后仍保留修订号，重新告警时不会退回旧版本。
            current.revision = previous == null ? 0 : previous.revision;
            if (current.alert || (previous != null && previous.alert)) current.revision++;
            windows.put(minute, current);
            if (!current.alert && (previous == null || !previous.alert)) return;
            JSONObject result = new JSONObject();
            result.put("ip", ip);
            result.put("alert_minute", minute);
            result.put("revision", current.revision);
            result.put("retracted", !current.alert);
            result.put("window_start", Instant.ofEpochMilli(current.start).toString());
            result.put("window_end", Instant.ofEpochMilli(current.end).toString());
            result.put("article_ids", current.articleIds);
            result.put("article_count", current.articleIds.size());
            result.put("click_count", current.clickCount);
            result.put("read_duration_sum_ms", current.durationSum);
            result.put("avg_read_duration_ms", (double) current.durationSum / current.clickCount);
            result.put("detect_time", Instant.now().toString());
            out.collect(result);
        }
    }

    /** 主键为分钟+IP；修正按 revision 覆盖，撤销保留主键以便单独查询。 */
    public static final String MYSQL_SQL =
            "INSERT INTO ip_alert(alert_minute_ms,ip,retracted,revision,window_start_ms,window_end_ms,"
                    + "article_count,click_count,avg_read_duration_ms,article_ids,detect_time,read_duration_sum_ms) "
                    + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE "
                    + "retracted=IF(VALUES(revision)>=revision,VALUES(retracted),retracted),"
                    + "window_start_ms=IF(VALUES(revision)>=revision,VALUES(window_start_ms),window_start_ms),"
                    + "window_end_ms=IF(VALUES(revision)>=revision,VALUES(window_end_ms),window_end_ms),"
                    + "article_count=IF(VALUES(revision)>=revision,VALUES(article_count),article_count),"
                    + "click_count=IF(VALUES(revision)>=revision,VALUES(click_count),click_count),"
                    + "avg_read_duration_ms=IF(VALUES(revision)>=revision,VALUES(avg_read_duration_ms),avg_read_duration_ms),"
                    + "article_ids=IF(VALUES(revision)>=revision,VALUES(article_ids),article_ids),"
                    + "detect_time=IF(VALUES(revision)>=revision,VALUES(detect_time),detect_time),"
                    + "read_duration_sum_ms=IF(VALUES(revision)>=revision,VALUES(read_duration_sum_ms),read_duration_sum_ms),"
                    + "revision=GREATEST(revision,VALUES(revision))";

    public static void bindMySql(PreparedStatement ps, JSONObject value) throws SQLException {
        ps.setLong(1, value.getLongValue("alert_minute"));
        ps.setString(2, value.getString("ip"));
        ps.setBoolean(3, value.getBooleanValue("retracted"));
        ps.setLong(4, value.getLongValue("revision"));
        if (value.getBooleanValue("retracted")) {
            ps.setNull(5, Types.BIGINT);
            ps.setNull(6, Types.BIGINT);
            ps.setNull(7, Types.INTEGER);
            ps.setNull(8, Types.INTEGER);
            ps.setNull(9, Types.DOUBLE);
            ps.setNull(10, Types.VARCHAR);
            ps.setNull(11, Types.VARCHAR);
            ps.setNull(12, Types.BIGINT);
        } else {
            ps.setLong(5, OffsetDateTime.parse(value.getString("window_start")).toInstant().toEpochMilli());
            ps.setLong(6, OffsetDateTime.parse(value.getString("window_end")).toInstant().toEpochMilli());
            ps.setInt(7, value.getIntValue("article_count"));
            ps.setInt(8, value.getIntValue("click_count"));
            ps.setDouble(9, value.getDoubleValue("avg_read_duration_ms"));
            ps.setString(10, value.getJSONArray("article_ids").toJSONString());
            ps.setString(11, value.getString("detect_time"));
            ps.setLong(12, value.getLongValue("read_duration_sum_ms"));
        }
    }
}

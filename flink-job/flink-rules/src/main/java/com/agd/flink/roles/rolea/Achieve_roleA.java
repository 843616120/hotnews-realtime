package com.agd.flink.roles.rolea;

import com.alibaba.fastjson.JSONObject;
import com.agd.flink.join.ArticleJoinBehavior;
import util.RoleStreamUtil;
import util.FlinkSinkUtil;
import util.FlinkMetricsUtil;
import util.FlinkRuntimeUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
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
import java.time.OffsetDateTime;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 规则 A：识别五分钟内点击量超过 1000 次的热点文章。
 *
 * <p>复用 Join 富化行为和事件时间，只对 click 做五分钟滑动窗口统计，
 * 步长一分钟。窗口内累加点击和最新文章信息，点击数大于 1000 才输出；
 * 晚到点击在允许时间内重新计算，超过时间则进入旁路供补算。</p>
 */
public class Achieve_roleA {
    private static final OutputTag<JSONObject> lateTag =
            new OutputTag<JSONObject>("role-a-late") {};

    /** 超过规则 A 窗口允许迟到时间的点击，供离线补算。 */
    public static OutputTag<JSONObject> lateTag() {
        return lateTag;
    }

    public static void main(String[] args) throws Exception {
        // Join 自己消费两条 Kafka 流并处理脏数据、迟到和未匹配行为；规则 A 只消费富化结果。
        // IDEA 本地运行使用 8083，避开 Docker JobManager 的 8081 和 Source 示例的 8082。
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, 8083);
        configuration.set(RestOptions.BIND_PORT, "8083");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(3);
        FlinkRuntimeUtil.configureCheckpointing(env, "HOTNEWS_CHECKPOINT_DIR");

        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env, "hotnews-role-a");

        // Join 补发行为时可能沿用文章时间戳，prepare 会恢复行为自身的事件时间。
        // 清洗明细按 event_id UPSERT；告警按窗口起点 + article_id UPSERT。
        SingleOutputStreamOperator<JSONObject> clean = RoleStreamUtil.prepare(joined);
        FlinkSinkUtil.sinkMySql(clean, RoleStreamUtil.MYSQL_SQL,
                RoleStreamUtil::bindMySql, null, "MySQL clean behavior");
        SingleOutputStreamOperator<JSONObject> result = buildRule(clean);

        // Join 的三类异常由 Join 写库；此处只记录超出规则 A 窗口保留期的点击。
        DataStream<JSONObject> lateEvents = result.getSideOutput(lateTag)
                .map(value -> RoleStreamUtil.audit(value, "ROLE_A_LATE"));
        FlinkSinkUtil.sinkMySql(lateEvents, RoleStreamUtil.AUDIT_MYSQL_SQL,
                RoleStreamUtil::bindAudit, null, "MySQL role A audit events");
        DataStream<JSONObject> measuredResult = FlinkMetricsUtil.measure(result, "role_a_result");
        FlinkSinkUtil.sinkMySql(measuredResult, MYSQL_SQL, Achieve_roleA::bindMySql,
                null, "MySQL article alerts");
        result.print("ROLE_A");
        result.getSideOutput(lateTag).print("ROLE_A_LATE");
        env.execute("Achieve_roleA");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        // Flink 的窗口负责触发和清理状态：每条点击进入五个重叠窗口。
        // 允许迟到 65 分钟，与当前 Join 的补算范围一致；迟到更新原窗口并再次输出最终计数。
        return joined.filter(value -> "click".equals(value.getString("action")))
                .keyBy(value -> value.getString("article_id"))
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .allowedLateness(Time.minutes(65))
                .sideOutputLateData(lateTag)
                .aggregate(new CountClicks(), new EmitHotArticles());
    }

    /**
     * 规则 A 自己声明 article_alert 表的 UPSERT 和字段顺序。
     * 公共 FlinkSinkUtil 只负责执行这段 SQL，不知道规则 A 的业务字段。
     */
    public static final String MYSQL_SQL =
            "INSERT INTO article_alert(window_start_ms,article_id,window_end_ms,title,"
                    + "category,click_count,detect_time) VALUES(?,?,?,?,?,?,?) "
                    + "ON DUPLICATE KEY UPDATE window_end_ms=VALUES(window_end_ms),"
                    + "title=VALUES(title),category=VALUES(category),"
                    + "click_count=GREATEST(click_count,VALUES(click_count)),"
                    + "detect_time=VALUES(detect_time)";

    /** 把规则 A 的结果按 article_alert 表列顺序写入 PreparedStatement。 */
    public static void bindMySql(PreparedStatement statement, JSONObject value) throws SQLException {
        statement.setLong(1, epoch(value, "window_start"));
        statement.setString(2, value.getString("article_id"));
        statement.setLong(3, epoch(value, "window_end"));
        statement.setString(4, value.getString("title"));
        statement.setString(5, value.getString("category"));
        statement.setLong(6, value.getLongValue("click_count"));
        statement.setString(7, value.getString("detect_time"));
    }

    private static long epoch(JSONObject value, String key) {
        return OffsetDateTime.parse(value.getString(key)).toInstant().toEpochMilli();
    }

    /** 每个 article_id + 窗口只保存累计计数和输出所需的文章字段，不保存所有点击明细。 */
    public static class ClickAccumulator {
        public long count;
        public int version;
        public long latestTime;
        public String title;
        public String category;
    }

    private static class CountClicks implements AggregateFunction<JSONObject, ClickAccumulator, ClickAccumulator> {
        @Override
        public ClickAccumulator createAccumulator() {
            return new ClickAccumulator();
        }

        @Override
        public ClickAccumulator add(JSONObject value, ClickAccumulator count) {
            long timestamp = RoleStreamUtil.eventTime(value);
            count.count++;
            int version = value.getIntValue("article_version");
            // 标题和分类取最高文章版本；同版本取事件时间最新的行为所携带的文章信息。
            if (count.count == 1 || version > count.version
                    || (version == count.version && timestamp >= count.latestTime)) {
                count.version = version;
                count.latestTime = timestamp;
                count.title = value.getString("title");
                count.category = value.getString("category");
            }
            return count;
        }

        @Override
        public ClickAccumulator getResult(ClickAccumulator count) {
            return count;
        }

        @Override
        public ClickAccumulator merge(ClickAccumulator left, ClickAccumulator right) {
            left.count += right.count;
            if (right.version > left.version
                    || (right.version == left.version && right.latestTime >= left.latestTime)) {
                left.version = right.version;
                left.latestTime = right.latestTime;
                left.title = right.title;
                left.category = right.category;
            }
            return left;
        }
    }

    private static class EmitHotArticles extends ProcessWindowFunction<ClickAccumulator, JSONObject, String, TimeWindow> {
        @Override
        public void process(String articleId, Context context, Iterable<ClickAccumulator> values,
                            Collector<JSONObject> out) {
            ClickAccumulator count = values.iterator().next();
            if (count.count <= 1000) {
                return;
            }
            // 窗口起止由 Flink 给出，结束边界不包含在本窗口内；detect_time 是实际触发时间。
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

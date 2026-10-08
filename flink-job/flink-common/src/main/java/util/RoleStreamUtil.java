package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 三条规则共用的二次幂等保护与行为时间恢复。生产入口的 Schema ETL
 * 已在 Join 前按 event_id 去重；这里防御 Checkpoint 回退/单独调用规则的重复输入。
 * 状态 TTL 按处理时间计算，生产用 24 小时；测试可传入较短 TTL 验证过期。
 */
public class RoleStreamUtil {
    private static final Duration EVENT_TIME_OUT_OF_ORDERNESS = Duration.ofSeconds(30);

    public static WatermarkStrategy<JSONObject> behaviorWatermarks() {
        return WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(EVENT_TIME_OUT_OF_ORDERNESS)
                .withTimestampAssigner((value, previous) -> eventTime(value))
                .withIdleness(Duration.ofSeconds(30));
    }

    /** Join 补发时输出可能沿用文章时间戳，改回行为时间戳，同时保留 Join 的 Watermark。 */
    public static SingleOutputStreamOperator<JSONObject> restoreBehaviorTime(
            SingleOutputStreamOperator<JSONObject> joined) {
        return joined.transform("Restore_Behavior_Event_Time", joined.getType(),
                new RestoreBehaviorTime());
    }

    private static class RestoreBehaviorTime extends AbstractStreamOperator<JSONObject>
            implements OneInputStreamOperator<JSONObject, JSONObject> {
        @Override
        public void processElement(StreamRecord<JSONObject> record) {
            output.collect(new StreamRecord<JSONObject>(
                    record.getValue(), eventTime(record.getValue())));
        }
        // AbstractStreamOperator 会原样转发上游 Watermark；此处不另造一条水位线。
    }

    /** Join 已恢复行为事件时间及 Watermark；规则入口仅做一次幂等保护。 */
    public static SingleOutputStreamOperator<JSONObject> prepare(DataStream<JSONObject> joined) {
        return prepare(joined, Time.hours(24));
    }

    public static SingleOutputStreamOperator<JSONObject> prepare(DataStream<JSONObject> joined, Time ttl) {
        SingleOutputStreamOperator<JSONObject> prepared = joined.keyBy(value -> value.getString("event_id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private transient ValueState<Boolean> seen;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Boolean> descriptor =
                                new ValueStateDescriptor<Boolean>("seen-event-id", Boolean.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(ttl).build());
                        seen = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, Context context, Collector<JSONObject> out)
                            throws Exception {
                        if (seen.value() == null) {
                            seen.update(true);
                            out.collect(value);
                        }
                    }
                });
        // 这里是进入规则前的统一清洗/去重边界，指标可与 Source 和 Sink 吞吐对照。
        return FlinkMetricsUtil.measure(prepared, "clean_behavior_records");
    }

    public static long eventTime(JSONObject value) {
        return OffsetDateTime.parse(value.getString("event_time")).toInstant().toEpochMilli();
    }

    /** 规则和状态程序超过在线补算保留期的数据，按类型写入 pipeline_event。 */
    public static final String AUDIT_MYSQL_SQL =
            "INSERT INTO pipeline_event(event_type,event_id,article_id,dirty_reason,payload) "
                    + "VALUES(?,?,?,?,?) ON DUPLICATE KEY UPDATE "
                    + "article_id=VALUES(article_id),dirty_reason=VALUES(dirty_reason),"
                    + "payload=VALUES(payload)";

    /** 给规则超期记录补充类别，Join 的三条旁路由 Join 自己落表。 */
    public static JSONObject audit(JSONObject value, String type) {
        JSONObject result = new JSONObject();
        result.putAll(value);
        result.put("exception_type", type);
        // 规则迟到旁路可能只携带原始业务字段；补充稳定原因码后，
        // pipeline_event 不会出现“知道是哪类异常但不知道为什么”的记录。
        if (result.getString("dirty_reason") == null
                || result.getString("dirty_reason").isEmpty()) {
            result.put("dirty_reason", type);
        }
        return result;
    }

    /** 按 pipeline_event 的五列顺序绑定审计记录，公共 JDBC Sink 只负责批量执行。 */
    public static void bindAudit(java.sql.PreparedStatement statement, JSONObject value)
            throws java.sql.SQLException {
        statement.setString(1, value.getString("exception_type"));
        statement.setString(2, value.getString("event_id"));
        statement.setString(3, value.getString("article_id"));
        statement.setString(4, value.getString("dirty_reason"));
        statement.setString(5, value.toJSONString());
    }

    /**
     * 清洗明细由公共工具负责发送，但 clean_behavior 的列定义属于 ETL 结果，
     * 因此 SQL 和字段顺序放在产生该结果的公共业务流旁边，而不是 Sink 工具里。
     */
    public static final String MYSQL_SQL =
            "INSERT INTO clean_behavior(event_id,user_id,article_id,action,ip,event_time,"
                    + "read_duration_ms,title,category,tags) VALUES(?,?,?,?,?,?,?,?,?,?) "
                    + "ON DUPLICATE KEY UPDATE user_id=VALUES(user_id),article_id=VALUES(article_id),"
                    + "action=VALUES(action),ip=VALUES(ip),event_time=VALUES(event_time),"
                    + "read_duration_ms=VALUES(read_duration_ms),title=VALUES(title),"
                    + "category=VALUES(category),tags=VALUES(tags)";

    /** 把清洗并富化后的行为按 clean_behavior 表列顺序绑定。 */
    public static void bindMySql(PreparedStatement statement, JSONObject value) throws SQLException {
        statement.setString(1, value.getString("event_id"));
        statement.setString(2, value.getString("user_id"));
        statement.setString(3, value.getString("article_id"));
        statement.setString(4, value.getString("action"));
        statement.setString(5, value.getString("ip"));
        statement.setString(6, value.getString("event_time"));
        statement.setInt(7, value.getIntValue("read_duration_ms"));
        statement.setString(8, value.getString("title"));
        statement.setString(9, value.getString("category"));
        statement.setString(10, value.getJSONArray("tags").toJSONString());
    }
}

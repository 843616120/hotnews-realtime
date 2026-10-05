import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * 三条规则共用的事件预处理：按 event_id 去重，再恢复行为事件时间。
 * 状态 TTL 按处理时间计算，生产用 24 小时；测试可传入较短 TTL 验证过期后重新接收。
 */
public class RoleStreamUtil {
    /** Join 补发行为时会沿用文章时间戳，这里先去重，再恢复行为事件时间。 */
    public static SingleOutputStreamOperator<JSONObject> prepare(DataStream<JSONObject> joined) {
        return prepare(joined, Time.hours(24));
    }

    static SingleOutputStreamOperator<JSONObject> prepare(DataStream<JSONObject> joined, Time ttl) {
        return joined.keyBy(value -> value.getString("event_id"))
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
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofHours(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject value, long previousTimestamp) {
                                        return OffsetDateTime.parse(value.getString("event_time"))
                                                .toInstant().toEpochMilli();
                                    }
                                })
                                .withIdleness(Duration.ofSeconds(30)));
    }

    public static long eventTime(JSONObject value) {
        return OffsetDateTime.parse(value.getString("event_time")).toInstant().toEpochMilli();
    }
}

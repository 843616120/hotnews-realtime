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
 * 三条规则共用的二次幂等保护与行为时间恢复。生产入口的 Schema ETL
 * 已在 Join 前按 event_id 去重；这里防御 Checkpoint 回退/单独调用规则的重复输入。
 * 状态 TTL 按处理时间计算，生产用 24 小时；测试可传入较短 TTL 验证过期。
 */
public class RoleStreamUtil {
    /*
     * 固定生成器样本的有效行为数据最大事件时间回退约为 3625 秒。
     * 65 分钟覆盖这段特殊的提前到达数据，并为随机边界留出余量。
     */
    private static final Duration EVENT_TIME_OUT_OF_ORDERNESS = Duration.ofMinutes(65);

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
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                                EVENT_TIME_OUT_OF_ORDERNESS)
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

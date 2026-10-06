import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Instant;

/**
 * 第六天专用 Kafka 负载消息转换器，不读取业务 Topic 的 JSON Schema。
 * 思路：Kafka 性能工具写入独立 Topic；用分区和位点生成唯一事件 ID，
 * 将 Kafka 消息时间戳保留下来，分别衡量 Kafka 积压与消息到 Sink 的时延。
 */
public class Day6KafkaRecordDeserializer implements KafkaRecordDeserializationSchema<JSONObject> {
    private final int keys;

    public Day6KafkaRecordDeserializer(int keys) {
        this.keys = keys;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<JSONObject> out)
            throws IOException {
        JSONObject event = new JSONObject();
        event.put("event_id", "day6-" + record.partition() + "-" + record.offset());
        event.put("article_id", String.format("article-%06d",
                Math.floorMod(record.offset(), keys)));
        event.put("title", "Day 6 isolated Kafka load");
        event.put("category", "benchmark");
        event.put("article_version", 1);
        event.put("action", "click");
        event.put("event_time", Instant.ofEpochMilli(record.timestamp()).toString());
        event.put("day6_emitted_at_ms", record.timestamp());
        out.collect(event);
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return TypeInformation.of(JSONObject.class);
    }
}

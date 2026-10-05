package util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return getKafkaSource(topic, groupId, false);
    }

    public static KafkaSource<String> getKafkaSource(String topic, String groupId, boolean bounded) {
        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId(groupId)
                .setTopics(topic)
                // 优先使用该消费组已提交的位点；首次消费时从最早的消息开始。
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("enable.auto.commit", "false")
                .setValueOnlyDeserializer(new SimpleStringSchema());
        if (bounded) {
            // 验收模式以作业启动时的 Topic 末尾为止，不直接读取 JSONL。
            builder.setBounded(OffsetsInitializer.latest());
        }
        return builder.build();
    }
}

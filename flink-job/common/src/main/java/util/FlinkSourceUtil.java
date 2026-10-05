package util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Kafka Source 构造工具。用法：常规任务沿用提交位点；有界的完整回放显式传
 * fromEarliest=true。思路：只改变新任务的起始位点，不修改任何 Topic 内容。
 */
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return getKafkaSource(topic, groupId, false);
    }

    public static KafkaSource<String> getKafkaSource(String topic, String groupId, boolean bounded) {
        return getKafkaSource(topic, groupId, bounded, false);
    }

    /** 固定批次验收可忽略消费组的历史提交位点，从 Topic 开头重新读取。 */
    public static KafkaSource<String> getKafkaSource(
            String topic, String groupId, boolean bounded, boolean fromEarliest) {
        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers(System.getenv().getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(fromEarliest ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
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

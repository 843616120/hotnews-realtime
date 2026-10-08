package util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 持续消费 Kafka。默认从消费组已提交位点继续，无位点时从最早数据开始。
 */
public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return getKafkaSource(topic, groupId,
                OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST));
    }

    /** 文章流可从最早位点加载文章状态；起点不同，但都会持续读取后续新消息。 */
    public static KafkaSource<String> getKafkaSource(
            String topic, String groupId, OffsetsInitializer startingOffsets) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(System.getenv().getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(startingOffsets)
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("enable.auto.commit", "false")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}

package util;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return kafkaSource;

    }
}

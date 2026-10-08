import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** 只读 Kafka broker 末端/消费组位点及保留策略；不订阅 Topic，也不提交位点。 */
public class KafkaProbe {
    public static void main(String[] args) throws Exception {
        String topic = args.length > 0 ? args[0] : "topic_behavior";
        String group = args.length > 1 ? args[1] : "hotnews-role-all-salted-behavior";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args.length > 2 ? args[2] : "localhost:9092");
        properties.put("request.timeout.ms", "10000");
        properties.put("default.api.timeout.ms", "15000");
        try (AdminClient admin = AdminClient.create(properties)) {
            List<TopicPartition> partitions = new ArrayList<>();
            admin.describeTopics(Collections.singleton(topic)).all().get(15, TimeUnit.SECONDS)
                    .get(topic).partitions().forEach(info -> partitions.add(new TopicPartition(topic, info.partition())));
            Map<TopicPartition, OffsetSpec> endRequests = new HashMap<>();
            Map<TopicPartition, OffsetSpec> beginRequests = new HashMap<>();
            for (TopicPartition partition : partitions) {
                endRequests.put(partition, OffsetSpec.latest());
                beginRequests.put(partition, OffsetSpec.earliest());
            }
            Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> ends =
                    admin.listOffsets(endRequests).all().get(15, TimeUnit.SECONDS);
            Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> begins =
                    admin.listOffsets(beginRequests).all().get(15, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata().get(15, TimeUnit.SECONDS);
            List<Map<String, Object>> rows = new ArrayList<>();
            long totalLag = 0;
            boolean complete = true;
            for (TopicPartition partition : partitions) {
                long end = ends.get(partition).offset();
                OffsetAndMetadata offset = committed.get(partition);
                Long lag = offset == null ? null : Math.max(0, end - offset.offset());
                if (lag == null) complete = false;
                else totalLag += lag;
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("partition", partition.partition());
                row.put("begin", begins.get(partition).offset());
                row.put("end", end);
                row.put("committed", offset == null ? null : offset.offset());
                row.put("committed_lag", lag);
                rows.add(row);
            }
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config config = admin.describeConfigs(Collections.singleton(resource)).all().get(15, TimeUnit.SECONDS)
                    .get(resource);
            Map<String, Object> report = new LinkedHashMap<>();
            report.put("captured_at", Instant.now().toString());
            report.put("topic", topic);
            report.put("group", group);
            report.put("retention.ms", config.get("retention.ms") == null ? null : config.get("retention.ms").value());
            report.put("retention.bytes", config.get("retention.bytes") == null ? null : config.get("retention.bytes").value());
            report.put("partitions", rows);
            report.put("total_committed_lag", complete ? totalLag : null);
            report.put("lag_note", complete ? "Checkpoint 提交位点口径" : "消费组暂无完整提交位点，不能将总 Lag 写为 0");
            System.out.println(JSON.toJSONString(report));
        }
    }
}

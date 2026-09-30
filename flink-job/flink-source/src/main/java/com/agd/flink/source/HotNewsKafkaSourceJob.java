package com.agd.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local smoke test for the two Kafka input topics.
 *
 * <p>The source records are counted and discarded instead of printed one by
 * one. Use the local Web UI metrics to inspect records in/out.</p>
 */
public class HotNewsKafkaSourceJob {

    private static final Logger LOG = LoggerFactory.getLogger(HotNewsKafkaSourceJob.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ARTICLE_TOPIC = "topic_article";
    private static final String BEHAVIOR_TOPIC = "topic_behavior";
    private static final String DEFAULT_GROUP_ID = "hotnews-local-idea";
    private static final int DEFAULT_WEB_PORT = 8082;

    public static void main(String[] args) throws Exception {
        JobOptions options = JobOptions.parse(args);
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, options.webPort);
        configuration.set(RestOptions.BIND_PORT, String.valueOf(options.webPort));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.enableCheckpointing(10_000L);

        LOG.info(
                "Starting hot-news Kafka source job: bootstrapServers={}, "
                        + "groupId={}, localWebUi=http://localhost:{}",
                options.bootstrapServers,
                options.groupId,
                options.webPort
        );

        KafkaSource<String> articleSource = buildSource(ARTICLE_TOPIC, options);
        KafkaSource<String> behaviorSource = buildSource(BEHAVIOR_TOPIC, options);

        DataStreamSource<String> articles = env.fromSource(
                articleSource,
                WatermarkStrategy.noWatermarks(),
                "topic_article-source"
        );
        DataStreamSource<String> behaviors = env.fromSource(
                behaviorSource,
                WatermarkStrategy.noWatermarks(),
                "topic_behavior-source"
        );

        articles
                .map(new CountingMapFunction("article"))
                .name("count-article-records")
                .uid("count-article-records")
                .addSink(new DiscardingSink<String>())
                .name("discard-article-records")
                .uid("discard-article-records");
        behaviors
                .map(new CountingMapFunction("behavior"))
                .name("count-behavior-records")
                .uid("count-behavior-records")
                .addSink(new DiscardingSink<String>())
                .name("discard-behavior-records")
                .uid("discard-behavior-records");

        env.execute("hotnews-kafka-source-smoke-test");
    }

    private static KafkaSource<String> buildSource(String topic, JobOptions options) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(options.bootstrapServers)
                .setTopics(topic)
                .setGroupId(options.groupId + "-" + topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private static final class CountingMapFunction extends RichMapFunction<String, String> {
        private final String streamName;
        private transient Counter recordsReceived;

        private CountingMapFunction(String streamName) {
            this.streamName = streamName;
        }

        @Override
        public void open(Configuration parameters) {
            recordsReceived = getRuntimeContext()
                    .getMetricGroup()
                    .addGroup("hotnews")
                    .addGroup(streamName)
                    .counter("records_received");
        }

        @Override
        public String map(String value) {
            recordsReceived.inc();
            return value;
        }
    }

    private static final class JobOptions {
        private final String bootstrapServers;
        private final String groupId;
        private final int webPort;

        private JobOptions(String bootstrapServers, String groupId, int webPort) {
            this.bootstrapServers = bootstrapServers;
            this.groupId = groupId;
            this.webPort = webPort;
        }

        private static JobOptions parse(String[] args) {
            String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
            String groupId = DEFAULT_GROUP_ID;
            int webPort = DEFAULT_WEB_PORT;

            for (int index = 0; index < args.length; index++) {
                switch (args[index]) {
                    case "--bootstrap-servers":
                        bootstrapServers = nextValue(args, ++index, "--bootstrap-servers");
                        break;
                    case "--group-id":
                        groupId = nextValue(args, ++index, "--group-id");
                        break;
                    case "--web-port":
                        webPort = Integer.parseInt(nextValue(args, ++index, "--web-port"));
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + args[index]);
                }
            }
            return new JobOptions(bootstrapServers, groupId, webPort);
        }

        private static String nextValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[index];
        }
    }
}

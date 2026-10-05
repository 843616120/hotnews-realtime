package com.agd.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/**
 * Kafka 输入层的最小示例。
 *
 * <p>本类只负责三件事：</p>
 * <ol>
 *     <li>创建两个 KafkaSource。</li>
 *     <li>把 Kafka 中的 JSON 文本读取成 Flink 的 DataStream&lt;String&gt;。</li>
 *     <li>把流连接到一个最小 Sink，提交并运行 Flink 作业。</li>
 * </ol>
 *
 * <p>JSON 解析、Schema 校验、对象转换、Watermark、双流 Join 和业务规则
 * 不放在这个最小 Source 示例中，后续可以在 flink-source 模块中继续拆分。</p>
 */
public class HotNewsKafkaSourceJob {

    // IDEA 在 Windows 宿主机运行时使用 localhost:9092。
    // 如果这个作业提交到 Docker Flink 集群，应改为 kafka:29092。
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // 两个 Kafka Topic：文章发布流和用户行为流。
    private static final String ARTICLE_TOPIC = "topic_article";
    private static final String BEHAVIOR_TOPIC = "topic_behavior";

    // Kafka 消费者组用于保存消费位点。
    private static final String ARTICLE_GROUP_ID = "hotnews-source-demo-article";
    private static final String BEHAVIOR_GROUP_ID = "hotnews-source-demo-behavior";

    // 本地 IDEA Web UI 使用 8082，避免和 Docker JobManager 的 8081 冲突。
    private static final int LOCAL_WEB_PORT = 8082;

    public static void main(String[] args) throws Exception {
        /*
         * 1. 创建 Flink 执行环境。
         *
         * Configuration 只用于给本地 MiniCluster 指定 Web UI 端口。
         * 如果不需要 IDEA 本地 Web UI，也可以直接使用：
         * StreamExecutionEnvironment.getExecutionEnvironment()
         */
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.PORT, LOCAL_WEB_PORT);
        configuration.set(RestOptions.BIND_PORT, String.valueOf(LOCAL_WEB_PORT));

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        /*
         * 2. 创建两个 KafkaSource。
         *
         * KafkaSource<String> 的含义是：
         * Kafka 每条消息的 value 会被反序列化成 Java String。
         * 当前使用 SimpleStringSchema，所以这里还没有变成 ArticleEvent
         * 或 BehaviorEvent 对象，value 仍然是原始 JSON 文本。
         */
        KafkaSource<String> articleSource = buildKafkaSource(
                ARTICLE_TOPIC,
                ARTICLE_GROUP_ID
        );
        KafkaSource<String> behaviorSource = buildKafkaSource(
                BEHAVIOR_TOPIC,
                BEHAVIOR_GROUP_ID
        );

        /*
         * 3. 把两个 KafkaSource 接入 Flink DataStream。
         *
         * 这里暂时使用 noWatermarks()，因为本类只验证 Kafka 读取。
         * 第二天任务中，需要在 JSON 转成带 eventTime 的对象之后，
         * 再分别使用 WatermarkStrategy 分配 Event Time 和 Watermark。
         */
        DataStreamSource<String> articleStream = env.fromSource(
                articleSource,
                WatermarkStrategy.noWatermarks(),
                "article-kafka-source"
        );
        DataStreamSource<String> behaviorStream = env.fromSource(
                behaviorSource,
                WatermarkStrategy.noWatermarks(),
                "behavior-kafka-source"
        );

        /*
         * 4. 添加最小 Sink，让 Flink 拓扑真正执行。
         *
         * Flink 的 DataStream API 是惰性的：只创建 Source 而不连接 Sink，
         * 作业不会形成完整的可执行拓扑。
         *
         * DiscardingSink 不打印、不计数、不写外部系统，只丢弃已经读取的
         * 消息。因此这个类适合先验证 Kafka 连接。需要临时查看原始 JSON
         * 时，可以把对应的 addSink(...) 改成 stream.print(...)，但不要用
         * 大批量正式数据长时间打印控制台。
         */
        articleStream
                .addSink(new DiscardingSink<String>())
                .name("discard-article-source-data");
        behaviorStream
                .addSink(new DiscardingSink<String>())
                .name("discard-behavior-source-data");

        /*
         * 5. 提交 Flink 作业。
         *
         * 作业启动后可以打开 http://localhost:8082 查看拓扑。
         * 当前拓扑只有两个 Kafka Source 和两个最小 Sink。
         */
        env.execute("hotnews-kafka-source");
    }

    /**
     * 创建一个 Kafka Source。
     *
     * @param topic   要读取的 Kafka Topic
     * @param groupId 该 Topic 对应的消费者组
     * @return Kafka 消息 value 为 String 的 KafkaSource
     */
    private static KafkaSource<String> buildKafkaSource(String topic,String groupId) {

        return KafkaSource.<String>builder()
                // Kafka 集群地址。
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                // 当前 Source 只读取一个 Topic。
                .setTopics(topic)
                // 不同流使用不同消费者组，避免两条流互相抢消息。
                .setGroupId(groupId)
                // 第一次启动从最早的消息开始读取。
                .setStartingOffsets(OffsetsInitializer.earliest())
                // Kafka value 的字节数组按 UTF-8 转成原始 JSON 字符串。
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;

/**
 * 第五天外部存储作业。用法：Docker 中 flink run -c Day5SinkJob 作业包；
 * --bounded 只用于固定批次验收；--plan 在本地仅构建执行图，不连接外部系统。
 * 思路：一份 ETL/Join 去重后的流同时驱动清洗明细、三条规则、MySQL 告警和 Redis 榜单。
 */
public class Day5SinkJob {
    public static void main(String[] args) throws Exception {
        boolean bounded = false;
        boolean planOnly = false;
        Integer batchSize = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bounded":
                    bounded = true;
                    break;
                case "--plan":
                    planOnly = true;
                    break;
                case "--mysql-batch-size":
                    if (++i >= args.length || batchSize != null) {
                        throw new IllegalArgumentException("--mysql-batch-size 需要一个整数值");
                    }
                    batchSize = Integer.parseInt(args[i]);
                    if (batchSize < 1 || batchSize > 10_000) {
                        throw new IllegalArgumentException("MySQL batch size 必须为 1..10000");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("未知参数: " + args[i]);
            }
        }
        if (bounded && planOnly) {
            throw new IllegalArgumentException("--bounded 与 --plan 不能同时使用");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setCheckpointTimeout(60_000L);
        checkpoints.setMinPauseBetweenCheckpoints(2_000L);
        checkpoints.setMaxConcurrentCheckpoints(1);
        checkpoints.setTolerableCheckpointFailureNumber(3);
        checkpoints.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        String checkpointDir = System.getenv("HOTNEWS_CHECKPOINT_DIR");
        if (checkpointDir != null && !checkpointDir.isEmpty()) {
            checkpoints.setCheckpointStorage(checkpointDir);
        }
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        SingleOutputStreamOperator<JSONObject> joined = ArticleJoinBehavior.createJoinedStream(
                env, bounded ? "hotnews-day5-verify" : "hotnews-day5", bounded, bounded);
        DataStream<JSONObject> exceptions = joined.getSideOutput(ArticleJoinBehavior.unmatchedBehaviorTag())
                .map(value -> exception(value, "UNMATCHED"))
                .union(joined.getSideOutput(ArticleJoinBehavior.lateDataTag())
                                .map(value -> exception(value, "LATE_DATA")),
                        joined.getSideOutput(ArticleJoinBehavior.dirtyBehaviorTag())
                                .map(value -> exception(value, "TEMPORAL_DIRTY")),
                        joined.getSideOutput(ArticleJoinBehavior.replayRequiredTag())
                                .map(value -> exception(value, "REPLAY_REQUIRED")));
        SingleOutputStreamOperator<JSONObject> clean = RoleStreamUtil.prepare(joined);
        clean.addSink(new Day5MySqlSink(Day5MySqlSink.Kind.DETAIL, batchSize)).name("MySQL clean behavior");

        SingleOutputStreamOperator<JSONObject> a = Achieve_roleA.buildRule(clean);
        Achieve_roleB.RuleStreams bStreams = Achieve_roleB.buildWithLate(clean);
        SingleOutputStreamOperator<JSONObject> b = bStreams.ranking;
        SingleOutputStreamOperator<JSONObject> c = Achieve_roleC.buildRule(clean);
        exceptions.union(a.getSideOutput(Achieve_roleA.lateTag())
                        .map(value -> exception(value, "ROLE_A_LATE")),
                        bStreams.expiredInputs.map(value -> exception(value, "ROLE_B_LATE_INPUT")),
                        b.getSideOutput(Achieve_roleB.lateTag())
                                .map(Day5SinkJob::lateRankScore),
                        c.getSideOutput(Achieve_roleC.lateTag())
                                .map(value -> exception(value, "ROLE_C_LATE")))
                .addSink(new Day5MySqlSink(Day5MySqlSink.Kind.PIPELINE_EVENT, batchSize))
                .name("MySQL queryable exceptions");
        a.addSink(new Day5MySqlSink(Day5MySqlSink.Kind.ARTICLE_ALERT, batchSize)).name("MySQL article alerts");
        b.addSink(new Day5MySqlSink(Day5MySqlSink.Kind.CATEGORY_RANK, batchSize)).name("MySQL category ranks");
        c.addSink(new Day5MySqlSink(Day5MySqlSink.Kind.IP_ALERT, batchSize)).name("MySQL IP alerts");
        b.filter(value -> value.getIntValue("rank") == 1)
                .addSink(new Day5RedisRankSink())
                .name("Redis atomic latest category top 5").setParallelism(1);
        if (planOnly) {
            System.out.println(env.getExecutionPlan());
        } else {
            env.execute("Day5SinkJob");
        }
    }

    /** 保留原事件和原因，并为 MySQL 异常查询加上稳定的类别。 */
    private static JSONObject exception(JSONObject value, String type) {
        JSONObject result = new JSONObject();
        result.putAll(value);
        result.put("exception_type", type);
        return result;
    }

    /** 第二阶段的文章得分没有行为 event_id，用文章 ID 和窗口组成稳定异常键。 */
    static JSONObject lateRankScore(Achieve_roleB.ArticleScore score) {
        JSONObject result = new JSONObject();
        result.put("exception_type", "ROLE_B_LATE");
        result.put("event_id", score.articleId + "-" + score.windowStart);
        result.put("article_id", score.articleId);
        result.put("category", score.category);
        result.put("window_start_ms", score.windowStart);
        result.put("score", score.count);
        result.put("dirty_reason", "RANK_STATE_EXPIRED");
        return result;
    }
}

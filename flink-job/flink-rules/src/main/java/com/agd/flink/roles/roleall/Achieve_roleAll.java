package com.agd.flink.roles.roleall;

import com.agd.flink.join.ArticleJoinBehavior;
import com.agd.flink.roles.rolea.Achieve_roleA;
import com.agd.flink.roles.roleb.Achieve_roleB;
import com.agd.flink.roles.rolec.Achieve_roleC;
import com.alibaba.fastjson.JSONObject;
import model.CategoryRankingSnapshot;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.FlinkMetricsUtil;
import util.FlinkRuntimeUtil;
import util.FlinkSinkUtil;
import util.RoleStreamUtil;

/**
 * 统一规则作业：一次 Join 后同时执行规则 A、规则 B 和规则 C。
 *
 * <p>三条规则仍然使用各自的事件时间窗口、迟到等待时间和输出表：
 * A 写 article_alert，B 写 category_rank 并更新 Redis，C 写 ip_alert。
 * 规则实现直接复用已经通过 SQL 校验的三个类，统一作业只负责组装数据流。</p>
 */
public class Achieve_roleAll {
    private static final int JOIN_SALTS = 8;

    public static void main(String[] args) throws Exception {
        // 先获取运行环境：IDEA 创建本地环境，flink run 使用客户端提供的集群环境。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (env instanceof LocalStreamEnvironment) {
            // REST 配置在创建本地环境时传入；env.configure 不会合并 REST 启动配置。
            // 集群提交不进入此分支，仍使用 Docker JobManager 的 8081。
            Configuration configuration = new Configuration();
            configuration.set(RestOptions.PORT, 10012);
            configuration.set(RestOptions.BIND_PORT, "10012");
            configuration.set(RestOptions.ADDRESS, "localhost");
            configuration.set(RestOptions.BIND_ADDRESS, "127.0.0.1");
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        }
        env.setParallelism(3);
        // 默认使用 HashMap；将环境变量改为 rocksdb 即可启动同一套作业做后端对比。
        // 两次运行只改变状态后端，Join、Watermark、窗口和 Sink 配置保持一致。
        String stateBackend = System.getenv().getOrDefault("HOTNEWS_STATE_BACKEND", "hashmap");
        if ("rocksdb".equalsIgnoreCase(stateBackend)) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        } else {
            env.setStateBackend(new HashMapStateBackend());
        }
        // B 的单并行度窗口在本机高负载时快照曾超过 60 秒；统一作业单独放宽超时。
        FlinkRuntimeUtil.configureCheckpointing(env, "HOTNEWS_CHECKPOINT_DIR", 30_000L, 180_000L);
        // Docker 单 TM 重启后需要重新注册 slot；原先每 5 秒重试，曾在 TM 注册前耗尽三次机会。
        // 统一作业改为每 30 秒重试一次，仍最多三次；只调整恢复等待，不改变业务状态和算子拓扑。
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(30)));

        // 新消费组避免优化版接着旧版的已提交位点只读取 Kafka 尾部。
        // 重复整批验收时可在 IDEA 设置新的 HOTNEWS_ROLE_ALL_GROUP；不得与旧作业同时写同一组结果表。
        String groupPrefix = System.getenv().getOrDefault("HOTNEWS_ROLE_ALL_GROUP", "hotnews-role-all-salted");
        // 统一作业只创建一次 Join。热点文章复制八份状态，行为按 event_id 分盐；
        // 行为仍只匹配一次，事件时间与 Watermark 设置保持不变。
        // 文章流和行为流的 Watermark、行为等待时间沿用 ArticleJoinBehavior 当前配置。
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env, groupPrefix, JOIN_SALTS);
        SingleOutputStreamOperator<JSONObject> clean = RoleStreamUtil.prepare(joined);

        // 清洗明细只写一次；三条规则共享同一份清洗后的 Join 结果。
        FlinkSinkUtil.sinkMySql(clean, RoleStreamUtil.MYSQL_SQL,
                RoleStreamUtil::bindMySql, null, "MySQL clean behavior");

        // 规则 A：五分钟滑动窗口、允许迟到 65 分钟，结果写 article_alert。
        SingleOutputStreamOperator<JSONObject> resultA = Achieve_roleA.buildRule(clean);
        sinkLate(resultA.getSideOutput(Achieve_roleA.lateTag()), "ROLE_A_LATE",
                "MySQL role A audit events");
        FlinkSinkUtil.sinkMySql(
                FlinkMetricsUtil.measure(resultA, "role_a_result"),
                Achieve_roleA.MYSQL_SQL, Achieve_roleA::bindMySql,
                null, "MySQL article alerts");
        resultA.print("ROLE_A");
        resultA.getSideOutput(Achieve_roleA.lateTag()).print("ROLE_A_LATE");

        // 规则 B：十分钟滚动窗口、允许迟到 65 分钟，MySQL 保存窗口结果，Redis 保存最新榜单。
        SingleOutputStreamOperator<JSONObject> resultB = Achieve_roleB.buildRule(clean);
        sinkLate(resultB.getSideOutput(Achieve_roleB.lateTag()), "ROLE_B_LATE",
                "MySQL role B audit events");
        DataStream<JSONObject> measuredB = FlinkMetricsUtil.measure(resultB, "role_b_result")
                .setParallelism(Achieve_roleB.RANK_PARALLELISM);
        Achieve_roleB.sinkRanks(measuredB);
        DataStream<CategoryRankingSnapshot> snapshots = resultB
                .filter(value -> value.getIntValue("rank") == 1)
                .map(Achieve_roleB::toRedisSnapshot);
        FlinkSinkUtil.sinkRedis(FlinkMetricsUtil.measure(snapshots, "role_b_redis_snapshots"),
                Achieve_roleB::writeRedisRanking, "Redis latest category ranking").setParallelism(1);
        resultB.print("ROLE_B");
        resultB.getSideOutput(Achieve_roleB.lateTag()).print("ROLE_B_LATE");

        // 规则 C：IP 一分钟回看、允许迟到 65 分钟，结果写 ip_alert。
        SingleOutputStreamOperator<JSONObject> resultC = Achieve_roleC.buildRule(clean);
        sinkLate(resultC.getSideOutput(Achieve_roleC.lateTag()), "ROLE_C_LATE",
                "MySQL role C audit events");
        FlinkSinkUtil.sinkMySql(
                FlinkMetricsUtil.measure(resultC, "role_c_result"),
                Achieve_roleC.MYSQL_SQL, Achieve_roleC::bindMySql,
                null, "MySQL IP alerts").setParallelism(1);
        resultC.print("ROLE_C");
        resultC.getSideOutput(Achieve_roleC.lateTag()).print("ROLE_C_LATE");

        // 主流检查部分过期窗口，旁路兜住各规则自身判定的迟到；请求按窗口去重。
        clean.union(replayEvents(resultA.getSideOutput(Achieve_roleA.lateTag()), "A"),
                        replayEvents(resultB.getSideOutput(Achieve_roleB.lateTag()), "B"),
                        replayEvents(resultC.getSideOutput(Achieve_roleC.lateTag()), "C"))
                .addSink(new RuleReplaySink()).name("MySQL complete window replay")
                .setParallelism(1).disableChaining();

        env.execute("Achieve_roleAll");
    }

    private static DataStream<JSONObject> replayEvents(DataStream<JSONObject> late, String rule) {
        return late.map(value -> {
            JSONObject copy = new JSONObject();
            copy.putAll(value);
            copy.put("replay_rule", rule);
            return copy;
        });
    }

    /** 三条规则的迟到旁路统一写入 pipeline_event，事件类型仍保留各自名称。 */
    private static void sinkLate(DataStream<JSONObject> late, String eventType, String sinkName) {
        FlinkSinkUtil.sinkMySql(
                late.map(value -> RoleStreamUtil.audit(value, eventType)),
                RoleStreamUtil.AUDIT_MYSQL_SQL, RoleStreamUtil::bindAudit,
                null, sinkName);
    }
}

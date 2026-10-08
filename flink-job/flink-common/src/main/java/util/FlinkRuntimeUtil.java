package util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Paths;

/**
 * 独立 Flink 入口的可靠性配置。
 *
 * <p>每个规则/状态程序都必须能够单独运行和恢复，因此 Checkpoint 配置不能
 * 只存在某个总入口中。本类只统一可靠性参数，不包含业务流、表名或 Sink 字段；
 * checkpointStorageEnv 由调用方选择；IDEA 未设置环境变量时落到当前工作目录的 checkpoints。</p>
 */
public final class FlinkRuntimeUtil {
    private FlinkRuntimeUtil() {
        // 配置工具不保存作业状态。
    }

    /** 设置 EXACTLY_ONCE、超时、并发、失败容忍、外置保留和固定延迟重启。 */
    public static void configureCheckpointing(
            StreamExecutionEnvironment env, String checkpointStorageEnv) {
        configureCheckpointing(env, checkpointStorageEnv, 5_000L, 60_000L);
    }

    /** 统一作业负载较重时可单独设置触发间隔和超时，其他入口保留原来的配置。 */
    public static void configureCheckpointing(StreamExecutionEnvironment env,
                                               String checkpointStorageEnv,
                                               long intervalMillis, long timeoutMillis) {
        env.enableCheckpointing(intervalMillis, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpoints = env.getCheckpointConfig();
        checkpoints.setCheckpointTimeout(timeoutMillis);
        checkpoints.setMinPauseBetweenCheckpoints(2_000L);
        checkpoints.setMaxConcurrentCheckpoints(1);
        checkpoints.setTolerableCheckpointFailureNumber(3);
        checkpoints.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        String storage = System.getenv(checkpointStorageEnv);
        if (storage == null || storage.trim().isEmpty()) {
            // 本地 MiniCluster 不依赖 JobManager 内存保存快照；Flink 会为各 Job 创建子目录。
            storage = Paths.get(System.getProperty("user.dir"), "checkpoints")
                    .toAbsolutePath().normalize().toUri().toString();
        }
        checkpoints.setCheckpointStorage(storage);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
    }
}

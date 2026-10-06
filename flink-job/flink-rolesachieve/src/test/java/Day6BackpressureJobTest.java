import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * 第六天压测参数边界测试。
 * 思路：禁止无界持续发压或过大并行度，并允许在不连接外部系统时检查执行图。
 */
public class Day6BackpressureJobTest {
    @Test
    public void parsesIsolatedConfiguration() {
        Day6BackpressureJob.Options options = Day6BackpressureJob.Options.parse(
                new String[]{"--rate", "5000", "--seconds", "12", "--delay-ms", "2",
                        "--sink-parallelism", "4", "--target", "redis", "--plan"});
        assertEquals(5000, options.rate);
        assertEquals(12, options.seconds);
        assertEquals(2, options.delayMs);
        assertEquals(4, options.sinkParallelism);
        assertEquals("redis", options.target);
    }

    @Test
    public void rejectsUnsafeRatesAndUnboundedRuns() {
        reject("--rate", "10001");
        reject("--seconds", "121");
        reject("--sink-parallelism", "0");
        reject("--delay-ms", "-1");
        reject("--keys", "10001");
        reject("--target", "production");
        reject("--rate");
        reject("--kafka-topic", "topic_behavior");
        reject("--kafka-topic", "hotnews-day6-a");
    }

    private static void reject(String... args) {
        try {
            Day6BackpressureJob.Options.parse(args);
            fail("无效参数必须报错");
        } catch (IllegalArgumentException expected) {
            // 参数在构图前被拒绝，不会启动 Source。
        }
    }
}

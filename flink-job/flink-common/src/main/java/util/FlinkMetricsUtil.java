package util;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Arrays;

/**
 * Flink 作业统一的吞吐指标算子。
 *
 * <p>压测必须观察真实 Source、算子和 Sink 的处理量，不能用一个独立的 sleep
 * 程序代替真实链路。本类提供一个不改变数据内容的身份 Map，在 Flink Web UI
 * 的 throughput 分组下暴露 records 和 records_per_second。业务程序只需把它
 * 放在需要观测的链路边界，数据仍然沿原来的拓扑继续流动。</p>
 */
public final class FlinkMetricsUtil {
    private FlinkMetricsUtil() {
        // 纯工厂类不保存任何作业运行状态。
    }

    /** 每个 Subtask 保存最近的处理延迟样本，供 Web UI 查询 p95，不写入业务状态。 */
    public static final class RollingP95 {
        private final long[] samples = new long[2048];
        private int count;
        private int next;

        public synchronized void record(long elapsedMs) {
            samples[next] = elapsedMs;
            next = (next + 1) % samples.length;
            if (count < samples.length) count++;
        }

        public synchronized long value() {
            if (count == 0) return 0;
            long[] sorted = Arrays.copyOf(samples, count);
            Arrays.sort(sorted);
            return sorted[(int) Math.ceil(count * 0.95) - 1];
        }
    }

    /** 在不改变数据的前提下为一个流增加记录数和滑动速率指标。 */
    public static <T> SingleOutputStreamOperator<T> measure(
            DataStream<T> stream, String metricName) {
        if (metricName == null || metricName.trim().isEmpty()) {
            throw new IllegalArgumentException("吞吐指标名称不能为空");
        }
        return stream.map(new ThroughputIdentityMap<T>(metricName))
                .name("throughput: " + metricName);
    }

    /** 每条记录同时增加累计计数和最近一分钟速率。 */
    private static final class ThroughputIdentityMap<T> extends RichMapFunction<T, T> {
        private final String metricName;
        private transient Counter records;
        private transient Meter rate;

        private ThroughputIdentityMap(String metricName) {
            this.metricName = metricName;
        }

        @Override
        public void open(Configuration parameters) {
            org.apache.flink.metrics.MetricGroup group = getRuntimeContext()
                    .getMetricGroup().addGroup("throughput", metricName);
            records = group.counter("records");
            rate = group.meter("records_per_second", new MeterView(60));
        }

        @Override
        public T map(T value) {
            records.inc();
            rate.markEvent();
            return value;
        }
    }
}

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 模拟外部 MySQL 明细写入或 Redis 请求的受控慢 Sink，不连接真实外部存储。
 * 思路：每条输入在 Sink 线程等待指定毫秒，记录消费量与 Source 到 Sink 的时延；
 * 对比零延迟及调大并行度的运行，观察反压如何沿链路向上游传递。
 */
public class Day6DelaySink extends RichSinkFunction<JSONObject> {
    private final int delayMs;
    private transient Counter consumed;
    private transient AtomicLong latency;

    public Day6DelaySink(int delayMs) {
        if (delayMs < 0 || delayMs > 1000) throw new IllegalArgumentException("Sink 延迟范围为 0..1000 ms");
        this.delayMs = delayMs;
    }

    @Override
    public void open(Configuration parameters) {
        consumed = getRuntimeContext().getMetricGroup().counter("day6_consumed");
        latency = new AtomicLong();
        getRuntimeContext().getMetricGroup().gauge("day6_last_latency_ms", () -> latency.get());
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        if (delayMs > 0) Thread.sleep(delayMs);
        latency.set(Math.max(0, System.currentTimeMillis() - value.getLongValue("day6_emitted_at_ms")));
        consumed.inc();
    }
}

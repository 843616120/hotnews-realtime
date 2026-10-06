import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Instant;

/**
 * 第六天隔离压测的限速输入源，不读取或写入生产 Kafka Topic。
 * 思路：每个并行子任务按全局目标速率分担序号，使用单调时钟控制发送节奏；
 * 每条事件带有实际发出时间，供下游计算 Source 到 Sink 的排队时延。
 */
public class Day6SyntheticSource extends RichParallelSourceFunction<JSONObject> {
    private final int rate;
    private final int seconds;
    private final int keys;
    private volatile boolean running = true;
    private transient Counter emitted;

    public Day6SyntheticSource(int rate, int seconds, int keys) {
        if (rate <= 0 || seconds <= 0 || keys <= 0)
            throw new IllegalArgumentException("速率、时长和键数量必须为正数");
        this.rate = rate;
        this.seconds = seconds;
        this.keys = keys;
    }

    @Override
    public void open(Configuration parameters) {
        emitted = getRuntimeContext().getMetricGroup().counter("day6_emitted");
    }

    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {
        int index = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        long total = (long) rate * seconds;
        long start = System.nanoTime();
        for (long sequence = index; running && sequence < total; sequence += parallelism) {
            long due = start + sequence * 1_000_000_000L / rate;
            while (running && System.nanoTime() < due) {
                long remaining = due - System.nanoTime();
                if (remaining > 1_000_000L) Thread.sleep(Math.min(remaining / 1_000_000L, 10));
                else Thread.yield();
            }
            if (!running) break;
            JSONObject event = new JSONObject();
            event.put("event_id", "day6-" + sequence);
            event.put("article_id", String.format("article-%06d", sequence % keys));
            event.put("title", "Day 6 isolated load");
            event.put("category", "benchmark");
            event.put("article_version", 1);
            event.put("action", "click");
            event.put("event_time", Instant.now().toString());
            synchronized (ctx.getCheckpointLock()) {
                event.put("day6_emitted_at_ms", System.currentTimeMillis());
                ctx.collect(event);
                emitted.inc();
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

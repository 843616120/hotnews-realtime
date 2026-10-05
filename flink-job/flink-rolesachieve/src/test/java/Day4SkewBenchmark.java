import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

/**
 * 第四天的独立本地倾斜实验，须显式用 -Dtest=Day4SkewBenchmark 运行。
 * 两份固定输入各 12000 条，分别让 90% 落到一篇文章或一个 IP；
 * 先测分组后的队列时延和 Subtask 分布，再核对两个独立作业的原始/分片窗口结果。
 */
public class Day4SkewBenchmark {
    private static final Map<String, List<ProbeResult>> METRICS =
            new ConcurrentHashMap<String, List<ProbeResult>>();

    /** 每个 Subtask 单独保存量和 p95；仅在本地 MiniCluster 同一 JVM 内汇总。 */
    public static class ProbeResult {
        public int subtask;
        public long count;
        public double p95Ms;
        public List<Long> latencies;
    }

    private static class Stamp extends RichMapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject value) {
            value.put("_source_ns", System.nanoTime());
            return value;
        }
    }

    private static class Probe extends RichMapFunction<JSONObject, JSONObject> {
        private final String mode;
        private transient List<Long> samples;

        private Probe(String mode) {
            this.mode = mode;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            samples = new ArrayList<Long>();
        }

        @Override
        public JSONObject map(JSONObject value) {
            samples.add(System.nanoTime() - value.getLongValue("_source_ns"));
            return value;
        }

        @Override
        public void close() {
            Collections.sort(samples);
            ProbeResult result = new ProbeResult();
            result.subtask = getRuntimeContext().getIndexOfThisSubtask();
            result.count = samples.size();
            result.latencies = samples;
            result.p95Ms = samples.isEmpty() ? 0 : samples.get((int) Math.ceil(samples.size() * .95) - 1) / 1e6;
            METRICS.computeIfAbsent(mode,
                    key -> Collections.synchronizedList(new ArrayList<ProbeResult>())).add(result);
        }
    }

    @Test
    public void compareSameHotInput() throws Exception {
        String backend = System.getProperty("day4.backend", "hashmap");
        if (!("hashmap".equals(backend) || "rocksdb".equals(backend))) {
            throw new IllegalArgumentException("day4.backend 仅支持 hashmap|rocksdb");
        }
        JSONObject report = new JSONObject(true);
        report.put("input_events_per_case", 12000);
        report.put("hot_key_events_per_case", 10800);
        report.put("latency_definition", "source timestamp -> keyed probe, excludes window completion");
        report.put("throughput_definition", "input events / local job wall clock including startup");
        report.put("backend", backend);
        report.put("article_heat", compare("article", fixedArticleInput()));
        report.put("ip_window", compare("ip", fixedIpInput()));
        Path root = Paths.get("").toAbsolutePath();
        while (root != null && !Files.isDirectory(root.resolve("generator"))) {
            root = root.getParent();
        }
        if (root == null) {
            throw new IllegalStateException("未找到项目根目录");
        }
        Path directory = root.resolve(Paths.get("tests", "day4", "results"));
        Files.createDirectories(directory);
        Path result = directory.resolve("local-skew-" + backend + "-"
                + Instant.now().toString().replace(':', '-') + ".json");
        Files.write(result, JSON.toJSONString(report, true).getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE_NEW);
        System.out.println("DAY4_REPORT " + result);
        System.out.println("DAY4_SUMMARY article=" + report.getJSONObject("article_heat")
                .getBooleanValue("window_results_equal") + " ip="
                + report.getJSONObject("ip_window").getBooleanValue("window_results_equal"));
    }

    private static JSONObject compare(String kind, List<JSONObject> input) throws Exception {
        measure(kind + "-warmup-baseline", kind, input.subList(0, 1200), false);
        measure(kind + "-warmup-optimized", kind, input.subList(0, 1200), true);
        JSONObject baseline1 = measure(kind + "-baseline-1", kind, input, false);
        JSONObject optimized1 = measure(kind + "-optimized-1", kind, input, true);
        JSONObject optimized2 = measure(kind + "-optimized-2", kind, input, true);
        JSONObject baseline2 = measure(kind + "-baseline-2", kind, input, false);
        assertEquals(baseline1.get("results"), optimized1.get("results"));
        assertEquals(baseline1.get("results"), optimized2.get("results"));
        assertEquals(baseline1.get("results"), baseline2.get("results"));
        JSONObject comparison = new JSONObject(true);
        comparison.put("baseline_runs", new JSONObject[] {baseline1, baseline2});
        comparison.put("optimized_runs", new JSONObject[] {optimized1, optimized2});
        comparison.put("window_results_equal", true);
        return comparison;
    }

    private static JSONObject measure(String mode, String kind, List<JSONObject> input, boolean optimized)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("state.backend.type", System.getProperty("day4.backend", "hashmap"));
        env.configure(conf);
        env.setParallelism(3);
        DataStream<JSONObject> source = env.fromCollection(input).map(new Stamp()).setParallelism(1);
        DataStream<JSONObject> probed = source.keyBy(value -> {
                    String key = "ip".equals(kind)
                            ? value.getString("ip") : value.getString("article_id");
                    return optimized ? key + "#"
                            + Math.floorMod(value.getString("event_id").hashCode(), 16) : key;
                })
                .map(new Probe(mode)).setParallelism(3);
        long start = System.nanoTime();
        List<JSONObject> results = new ArrayList<JSONObject>();
        DataStream<JSONObject> prepared = RoleStreamUtil.prepare(probed);
        DataStream<JSONObject> output = "ip".equals(kind)
                ? IpWindowStateJob.buildRule(prepared, optimized)
                : ArticleHeatStateJob.buildRule(prepared, optimized);
        try (CloseableIterator<JSONObject> records = output.executeAndCollect()) {
            while (records.hasNext()) {
                JSONObject result = records.next();
                result.remove("detect_time");
                results.add(result);
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        results.sort(Comparator.comparing(item ->
                item.getString("window_start") + "|" + ("ip".equals(kind)
                        ? item.getString("ip") : item.getString("article_id"))));
        List<ProbeResult> subtasks = METRICS.remove(mode);
        if (subtasks == null || subtasks.size() != 3) {
            throw new IllegalStateException("未采集到全部三个本地 Subtask 的探针数据");
        }
        subtasks.sort(Comparator.comparingInt(item -> item.subtask));
        List<Long> allLatencies = new ArrayList<Long>();
        for (ProbeResult item : subtasks) {
            allLatencies.addAll(item.latencies);
            item.latencies = null;
        }
        Collections.sort(allLatencies);
        JSONObject report = new JSONObject(true);
        report.put("elapsed_seconds", seconds);
        report.put("events_per_second", input.size() / seconds);
        report.put("p95_source_to_keyed_ms",
                allLatencies.get((int) Math.ceil(allLatencies.size() * .95) - 1) / 1e6);
        report.put("subtasks", subtasks);
        report.put("results", results);
        return report;
    }

    private static List<JSONObject> fixedArticleInput() {
        List<JSONObject> input = new ArrayList<JSONObject>();
        for (int i = 0; i < 12000; i++) {
            JSONObject item = new JSONObject();
            item.put("event_id", "bench-" + i);
            item.put("article_id", i < 10800 ? "article-hot" : "article-" + (i % 10));
            item.put("title", "benchmark article");
            item.put("category", "technology");
            item.put("article_version", 1);
            item.put("action", "click");
            item.put("ip", "10.0.0." + (i % 10));
            item.put("read_duration_ms", 1000);
            item.put("event_time", Instant.parse("2026-09-27T00:04:00Z")
                    .plusMillis(i % 120000).toString());
            input.add(item);
        }
        return input;
    }

    private static List<JSONObject> fixedIpInput() {
        List<JSONObject> input = fixedArticleInput();
        for (int i = 0; i < input.size(); i++) {
            JSONObject value = input.get(i);
            value.put("ip", i < 10800 ? "10.0.0.1" : "10.0.0." + (i % 10 + 2));
            value.put("article_id", "article-" + (i % 80));
        }
        return input;
    }
}

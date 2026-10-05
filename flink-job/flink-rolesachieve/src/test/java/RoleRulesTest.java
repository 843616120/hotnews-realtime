import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoleRulesTest {
    @Test
    public void roleAOnlyFiresAboveOneThousandAndRespectsWindowEnd() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        addClicks(input, "under", "10.0.0.1", 999, 1000, "2026-09-27T00:04:59.000Z");
        addClicks(input, "equal", "10.0.0.2", 1000, 1000, "2026-09-27T00:04:59.000Z");
        addClicks(input, "over", "10.0.0.3", 1001, 1000, "2026-09-27T00:04:59.000Z");
        input.add(record("over", "10.0.0.3", "share", "extra", 1000, "2026-09-27T00:04:59.000Z"));
        input.add(record("under", "10.0.0.1", "click", "edge", 1000, "2026-09-27T00:05:00.000Z"));

        List<JSONObject> output = run(input, Achieve_roleA::buildRule);
        assertEquals(5, output.size());
        for (JSONObject result : output) {
            assertEquals("over", result.getString("article_id"));
            assertEquals(1001L, result.getLongValue("click_count"));
            assertTrue(result.containsKey("detect_time"));
            assertEquals(300000L, java.time.Instant.parse(result.getString("window_end")).toEpochMilli()
                    - java.time.Instant.parse(result.getString("window_start")).toEpochMilli());
        }
    }

    @Test
    public void roleBRanksAllActionsAndIgnoresDuplicateEventId() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        input.add(record("alpha", "10.0.0.1", "click", "1", 1000, "2026-09-27T00:09:59.000Z"));
        input.add(record("alpha", "10.0.0.1", "share", "2", 1000, "2026-09-27T00:09:59.000Z"));
        input.add(record("alpha", "10.0.0.1", "comment", "3", 1000, "2026-09-27T00:09:59.000Z"));
        input.add(record("alpha", "10.0.0.1", "click", "1", 1000, "2026-09-27T00:09:59.000Z"));
        input.add(record("beta", "10.0.0.2", "click", "4", 1000, "2026-09-27T00:09:59.000Z"));
        input.add(record("alpha", "10.0.0.1", "click", "5", 1000, "2026-09-27T00:10:00.000Z"));
        for (String category : new String[]{"finance", "sports", "science", "world", "health", "entertainment"}) {
            JSONObject item = record(category, "10.0.0.2", "click", category,
                    1000, "2026-09-27T00:09:59.000Z");
            item.put("category", category);
            input.add(item);
        }

        List<JSONObject> output = run(input, Achieve_roleB::buildRule);
        JSONObject first = null;
        int firstWindowCount = 0;
        for (JSONObject result : output) {
            if ("2026-09-27T00:00:00Z".equals(result.getString("window_start"))) {
                firstWindowCount++;
                if (result.getIntValue("rank") == 1) {
                    first = result;
                }
            }
        }
        assertEquals(5, firstWindowCount);
        assertTrue(first != null);
        assertEquals("technology", first.getString("category"));
        assertEquals(4L, first.getLongValue("score"));
        assertEquals(2, first.getJSONArray("top_articles").size());
    }

    @Test
    public void roleCRequiresFiftyOneDistinctArticlesAndShortAverage() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        addClicks(input, "unique-", "10.0.0.1", 51, 1000, "2026-09-27T00:00:30.000Z");
        input.add(record("unique-0", "10.0.0.1", "click", "unique-0", 1000,
                "2026-09-27T00:00:30.000Z"));
        addClicks(input, "low-", "10.0.0.4", 49, 1000, "2026-09-27T00:00:30.000Z");
        addClicks(input, "other-", "10.0.0.2", 50, 1000, "2026-09-27T00:00:30.000Z");
        addClicks(input, "slow-", "10.0.0.3", 51, 2000, "2026-09-27T00:00:30.000Z");
        addClicks(input, "fast-", "10.0.0.5", 51, 1999, "2026-09-27T00:00:30.000Z");

        List<JSONObject> output = run(input, Achieve_roleC::buildRule);
        assertEquals(2, output.size());
        output.sort(Comparator.comparing(item -> item.getString("ip")));
        assertEquals("10.0.0.1", output.get(0).getString("ip"));
        assertEquals(51, output.get(0).getIntValue("article_count"));
        assertEquals(51, output.get(0).getIntValue("click_count"));
        assertEquals(1000.0, output.get(0).getDoubleValue("avg_read_duration_ms"), 0.001);
        assertEquals("10.0.0.5", output.get(1).getString("ip"));
        assertEquals(1999.0, output.get(1).getDoubleValue("avg_read_duration_ms"), 0.001);
    }

    @Test
    public void independentIpStateMatchesSaltedModeAtThresholds() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        addClicks(input, "under-", "10.0.0.4", 49, 1000, "2026-09-27T00:00:30Z");
        addClicks(input, "equal-", "10.0.0.2", 50, 1000, "2026-09-27T00:00:30Z");
        addClicks(input, "slow-", "10.0.0.3", 51, 2000, "2026-09-27T00:00:30Z");
        addClicks(input, "fast-", "10.0.0.5", 51, 1999, "2026-09-27T00:00:30Z");
        input.add(record("fast-0", "10.0.0.5", "click", "fast-0", 1999,
                "2026-09-27T00:00:30Z"));
        List<JSONObject> baseline = run(input, joined -> IpWindowStateJob.buildRule(joined, false));
        List<JSONObject> optimized = run(input, joined -> IpWindowStateJob.buildRule(joined, true));
        baseline.forEach(item -> item.remove("detect_time"));
        optimized.forEach(item -> item.remove("detect_time"));
        assertEquals(baseline, optimized);
        assertEquals(1, baseline.size());
        assertEquals(51, baseline.get(0).getJSONArray("article_ids").size());
        assertEquals(51, baseline.get(0).getLongValue("click_count"));
        assertEquals(1999.0, baseline.get(0).getDoubleValue("avg_read_duration_ms"), .001);
        assertEquals("2026-09-27T00:00:00Z", baseline.get(0).getString("window_start"));
        assertEquals("2026-09-27T00:01:00Z", baseline.get(0).getString("window_end"));
    }

    @Test
    public void saltedArticleWindowsMatchBaselineWithDuplicatesAndWindowEdges() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        addClicks(input, "hot", "10.0.0.1", 1001, 1000, "2026-09-27T00:04:59.000Z");
        input.add(record("hot", "10.0.0.1", "click", "hot0", 1000,
                "2026-09-27T00:04:59.000Z"));
        addClicks(input, "cold", "10.0.0.2", 50, 1000, "2026-09-27T00:05:00.000Z");
        List<JSONObject> baseline = run(input, joined -> ArticleHeatStateJob.buildRule(joined, false));
        List<JSONObject> optimized = run(input, joined -> ArticleHeatStateJob.buildRule(joined, true));
        baseline.forEach(item -> item.remove("detect_time"));
        optimized.forEach(item -> item.remove("detect_time"));
        Comparator<JSONObject> byWindow = Comparator.comparing(item -> item.getString("window_start"));
        baseline.sort(byWindow);
        optimized.sort(byWindow);
        assertEquals(baseline, optimized);
        assertEquals(5, optimized.size());
    }

    @Test
    public void deduplicationAcceptsSameIdAgainAfterTestTtl() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JSONObject value = record("hot", "10.0.0.1", "click", "same-id", 1000,
                "2026-09-27T00:00:00Z");
        List<JSONObject> input = new ArrayList<JSONObject>();
        input.add(value);
        input.add(value);
        input.add(value);
        DataStream<JSONObject> delayed = env.fromCollection(input).map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    private int index;
                    @Override
                    public JSONObject map(JSONObject item) throws Exception {
                        if (++index == 3) {
                            Thread.sleep(350);
                        }
                        return item;
                    }
                });
        int emitted = 0;
        try (CloseableIterator<JSONObject> results =
                     RoleStreamUtil.prepare(delayed, Time.milliseconds(100)).executeAndCollect()) {
            while (results.hasNext()) {
                results.next();
                emitted++;
            }
        }
        assertEquals(2, emitted);
    }

    private static List<JSONObject> run(List<JSONObject> input,
                                         Function<DataStream<JSONObject>, DataStream<JSONObject>> rule)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<JSONObject> output = new ArrayList<JSONObject>();
        try (CloseableIterator<JSONObject> results =
                     rule.apply(RoleStreamUtil.prepare(env.fromCollection(input))).executeAndCollect()) {
            while (results.hasNext()) {
                output.add(results.next());
            }
        }
        return output;
    }

    private static void addClicks(List<JSONObject> input, String articlePrefix, String ip,
                                  int count, int duration, String timestamp) {
        for (int i = 0; i < count; i++) {
            input.add(record(articlePrefix + (articlePrefix.endsWith("-") ? i : ""), ip,
                    "click", articlePrefix + i, duration, timestamp));
        }
    }

    private static JSONObject record(String article, String ip, String action,
                                     String id, int duration, String timestamp) {
        JSONObject value = new JSONObject();
        value.put("event_id", id);
        value.put("article_id", article);
        value.put("title", "文章" + article);
        value.put("category", "technology");
        value.put("action", action);
        value.put("ip", ip);
        value.put("event_time", timestamp);
        value.put("read_duration_ms", duration);
        value.put("article_version", 1);
        return value;
    }
}

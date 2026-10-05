import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
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
        assertEquals(5, first.getJSONArray("ranking").size());
        assertEquals(10L, first.getLongValue("revision"));
    }

    @Test
    public void joinWaitsForInitialPublicationAndSendsEarlierBehaviorToDirtyData() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JSONObject publish = new JSONObject();
        publish.put("article_id", "article-000001");
        publish.put("event_type", "publish");
        publish.put("version", 1);
        publish.put("event_time", "2026-09-27T00:01:00Z");
        JSONObject update = new JSONObject();
        update.putAll(publish);
        update.put("event_type", "update");
        update.put("version", 2);
        update.put("event_time", "2026-09-27T00:10:00Z");
        assertEquals("publish", publish.getString("event_type"));
        assertEquals(1, publish.getIntValue("version"));
        assertEquals("update", update.getString("event_type"));
        JSONObject tooEarly = record("article-000001", "10.0.0.1", "click", "early", 1000,
                "2026-09-27T00:00:59Z");
        JSONObject valid = record("article-000001", "10.0.0.1", "click", "valid", 1000,
                "2026-09-27T00:02:00Z");
        SingleOutputStreamOperator<JSONObject> joined = ArticleJoinBehavior.joinCleanStreams(
                env.fromElements(tooEarly, valid), env.fromElements(update, publish), true);
        DataStream<String> results = joined.map(value -> "JOIN:" + value.getString("event_id"))
                .union(joined.getSideOutput(ArticleJoinBehavior.dirtyBehaviorTag())
                        .map(value -> "DIRTY:" + value.getString("event_id") + ":"
                                + value.getString("dirty_reason")));
        List<String> output = new ArrayList<String>();
        try (CloseableIterator<String> iterator = results.executeAndCollect()) {
            while (iterator.hasNext()) {
                output.add(iterator.next());
            }
        }
        output.sort(String::compareTo);
        assertEquals(java.util.Arrays.asList(
                "DIRTY:early:BEFORE_INITIAL_PUBLICATION", "JOIN:valid"), output);
    }

    @Test
    public void boundedJoinReportsMissingInitialPublication() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JSONObject update = new JSONObject();
        update.put("article_id", "article-000001");
        update.put("event_type", "update");
        update.put("version", 2);
        update.put("event_time", "2026-09-27T00:10:00Z");
        JSONObject behavior = record("article-000001", "10.0.0.1", "click", "missing-publish",
                1000, "2026-09-27T00:11:00Z");
        SingleOutputStreamOperator<JSONObject> joined = ArticleJoinBehavior.joinCleanStreams(
                env.fromElements(behavior), env.fromElements(update), true);
        List<String> unmatched = new ArrayList<String>();
        try (CloseableIterator<JSONObject> results =
                     joined.getSideOutput(ArticleJoinBehavior.unmatchedBehaviorTag())
                             .executeAndCollect()) {
            while (results.hasNext()) {
                unmatched.add(results.next().getString("event_id"));
            }
        }
        assertEquals(1, unmatched.size());
        assertEquals("missing-publish", unmatched.get(0));
    }

    @Test
    public void etlRemovesDuplicatesBeforeJoinAndPendingState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JSONObject publish = publication("article-000001", "2026-09-27T00:01:00Z");
        JSONObject behavior = record("article-000001", "10.0.0.1", "click", "same-id",
                1000, "2026-09-27T00:02:00Z");
        SingleOutputStreamOperator<JSONObject> joined = ArticleJoinBehavior.joinCleanStreams(
                ArticleJoinBehavior.deduplicateBehaviors(env.fromElements(behavior, behavior)),
                env.fromElements(publish).map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        Thread.sleep(300);
                        return value;
                    }
                }), true);
        DataStream<String> results = joined.map(value -> "JOIN:" + value.getString("event_id"))
                .union(joined.getSideOutput(ArticleJoinBehavior.unmatchedBehaviorTag())
                                .map(value -> "UNMATCHED:" + value.getString("event_id")),
                        joined.getSideOutput(ArticleJoinBehavior.replayRequiredTag())
                                .map(value -> "REPLAY:" + value.getString("event_id")));
        List<String> output = new ArrayList<String>();
        try (CloseableIterator<String> iterator = results.executeAndCollect()) {
            while (iterator.hasNext()) {
                output.add(iterator.next());
            }
        }
        output.sort(String::compareTo);
        assertEquals(java.util.Arrays.asList("JOIN:same-id", "UNMATCHED:same-id"), output);
    }

    @Test
    public void allQueuedBehaviorsForSameArticleAreRejoined() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<JSONObject> behaviors = new ArrayList<JSONObject>();
        for (int i = 0; i < 80; i++) {
            behaviors.add(record("article-000328", "10.0.0.1", "click", "queued-" + i,
                    1000, "2026-09-27T00:02:00Z"));
        }
        SingleOutputStreamOperator<JSONObject> joined = ArticleJoinBehavior.joinCleanStreams(
                env.fromCollection(behaviors),
                env.fromElements(publication("article-000328", "2026-09-27T00:01:00Z"))
                        .map(new RichMapFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject map(JSONObject value) throws Exception {
                                Thread.sleep(800);
                                return value;
                            }
                        }), true);
        DataStream<String> results = joined.map(value -> "JOIN:" + value.getString("event_id"))
                .union(joined.getSideOutput(ArticleJoinBehavior.rejoinedBehaviorTag())
                                .map(value -> "REJOIN:" + value.getString("event_id")),
                        joined.getSideOutput(ArticleJoinBehavior.unmatchedBehaviorTag())
                                .map(value -> "UNMATCHED:" + value.getString("event_id")));
        List<String> output = new ArrayList<String>();
        try (CloseableIterator<String> iterator = results.executeAndCollect()) {
            while (iterator.hasNext()) {
                output.add(iterator.next());
            }
        }
        assertEquals(240, output.size());
        for (int i = 0; i < 80; i++) {
            assertTrue(output.contains("JOIN:queued-" + i));
            assertTrue(output.contains("REJOIN:queued-" + i));
            assertTrue(output.contains("UNMATCHED:queued-" + i));
        }
    }

    @Test
    public void watermarkedLateBehaviorStillJoinsAndIsObservable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JSONObject first = record("article-000001", "10.0.0.1", "click", "on-time",
                1000, "2026-09-27T00:10:00Z");
        JSONObject late = record("article-000001", "10.0.0.1", "click", "late",
                1000, "2026-09-27T00:02:00Z");
        DataStream<JSONObject> behaviorSource = env.addSource(new SourceFunction<JSONObject>() {
            @Override
            public void run(SourceContext<JSONObject> ctx) throws Exception {
                ctx.collectWithTimestamp(first, RoleStreamUtil.eventTime(first));
                ctx.emitWatermark(new Watermark(java.time.Instant.parse(
                        "2026-09-27T00:11:00Z").toEpochMilli()));
                Thread.sleep(350);
                ctx.collectWithTimestamp(late, RoleStreamUtil.eventTime(late));
            }

            @Override
            public void cancel() {
            }
        });
        SingleOutputStreamOperator<JSONObject> joined = ArticleJoinBehavior.joinCleanStreams(
                behaviorSource, env.fromElements(publication("article-000001",
                        "2026-09-27T00:01:00Z")), true);
        DataStream<String> results = joined.map(value -> "JOIN:" + value.getString("event_id"))
                .union(joined.getSideOutput(ArticleJoinBehavior.lateDataTag())
                        .map(value -> "LATE:" + value.getString("stream") + ":"
                                + value.getString("event_id")));
        List<String> output = new ArrayList<String>();
        try (CloseableIterator<String> iterator = results.executeAndCollect()) {
            while (iterator.hasNext()) {
                output.add(iterator.next());
            }
        }
        output.sort(String::compareTo);
        assertEquals(java.util.Arrays.asList(
                "JOIN:late", "JOIN:on-time", "LATE:behavior:late"), output);
    }

    @Test
    public void lateInitialArticleRejoinsPendingBehaviorAfterWatermark() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JSONObject behavior = record("article-000001", "10.0.0.1", "click", "wait",
                1000, "2026-09-27T00:02:00Z");
        JSONObject publish = publication("article-000001", "2026-09-27T00:01:00Z");
        DataStream<JSONObject> behaviorSource = env.addSource(new SourceFunction<JSONObject>() {
            @Override
            public void run(SourceContext<JSONObject> ctx) throws Exception {
                ctx.collectWithTimestamp(behavior, RoleStreamUtil.eventTime(behavior));
                ctx.emitWatermark(new Watermark(java.time.Instant.parse(
                        "2026-09-27T00:11:00Z").toEpochMilli()));
            }

            @Override
            public void cancel() {
            }
        });
        DataStream<JSONObject> articleSource = env.addSource(new SourceFunction<JSONObject>() {
            @Override
            public void run(SourceContext<JSONObject> ctx) throws Exception {
                ctx.emitWatermark(new Watermark(java.time.Instant.parse(
                        "2026-09-27T00:11:00Z").toEpochMilli()));
                Thread.sleep(350);
                ctx.collectWithTimestamp(publish, RoleStreamUtil.eventTime(publish));
            }

            @Override
            public void cancel() {
            }
        });
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.joinCleanStreams(behaviorSource, articleSource, true);
        DataStream<String> results = joined.map(value -> "JOIN:" + value.getString("event_id"))
                .union(joined.getSideOutput(ArticleJoinBehavior.unmatchedBehaviorTag())
                                .map(value -> "UNMATCHED:" + value.getString("event_id")),
                        joined.getSideOutput(ArticleJoinBehavior.lateDataTag())
                                .map(value -> "LATE:" + value.getString("stream")),
                        joined.getSideOutput(ArticleJoinBehavior.replayRequiredTag())
                                .map(value -> "REPLAY:" + value.getString("event_id")));
        List<String> output = new ArrayList<String>();
        try (CloseableIterator<String> iterator = results.executeAndCollect()) {
            while (iterator.hasNext()) {
                output.add(iterator.next());
            }
        }
        output.sort(String::compareTo);
        assertEquals(java.util.Arrays.asList(
                "JOIN:wait", "LATE:article", "UNMATCHED:wait"), output);
    }

    @Test
    public void pendingTtlExpiryRequestsReplayInsteadOfSilentLoss() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        JSONObject behavior = record("article-000001", "10.0.0.1", "click", "wait",
                1000, "2026-09-27T00:02:00Z");
        DataStream<JSONObject> behaviorSource = env.fromElements(behavior);
        DataStream<JSONObject> articleSource = env.fromElements(
                publication("article-000001", "2026-09-27T00:01:00Z"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        Thread.sleep(600);
                        return value;
                    }
                });
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.joinCleanStreams(behaviorSource, articleSource, false, 200L);
        DataStream<String> results = joined.map(value -> "JOIN:" + value.getString("event_id"))
                .union(joined.getSideOutput(ArticleJoinBehavior.unmatchedBehaviorTag())
                                .map(value -> "UNMATCHED:" + value.getString("event_id")),
                        joined.getSideOutput(ArticleJoinBehavior.replayRequiredTag())
                                .map(value -> "REPLAY:" + value.getString("event_id")));
        List<String> output = new ArrayList<String>();
        try (CloseableIterator<String> iterator = results.executeAndCollect()) {
            while (iterator.hasNext()) {
                output.add(iterator.next());
            }
        }
        output.sort(String::compareTo);
        assertEquals(java.util.Arrays.asList("REPLAY:wait", "UNMATCHED:wait"), output);
    }

    /** 构造同一文章的首版发布事件，供时序关联测试复用。 */
    private static JSONObject publication(String articleId, String timestamp) {
        JSONObject value = new JSONObject();
        value.put("article_id", articleId);
        value.put("event_type", "publish");
        value.put("version", 1);
        value.put("event_time", timestamp);
        value.put("title", "测试文章");
        value.put("category", "science");
        return value;
    }

    @Test
    public void roleALateClickCrossesThresholdAfterWindowCloses() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        addClicks(input, "hot", "10.0.0.1", 1000, 1000, "2026-09-27T00:04:00Z");
        input.add(record("hot", "10.0.0.1", "click", "late-hot", 1000,
                "2026-09-27T00:04:00Z"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<JSONObject> output = new ArrayList<JSONObject>();
        try (CloseableIterator<JSONObject> results = Achieve_roleA.buildRule(
                withWatermarkBefore(env, input, 1000, "2026-09-27T00:10:00Z"))
                .executeAndCollect()) {
            while (results.hasNext()) {
                output.add(results.next());
            }
        }
        assertEquals(5, output.size());
        for (JSONObject result : output) {
            assertEquals(1001L, result.getLongValue("click_count"));
        }
    }

    @Test
    public void roleCLateClickMovesFirstAlertThenRetractsIt() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        for (int index = 0; index < 50; index++) {
            input.add(record("article-" + index, "10.0.0.9", "click", "click-" + index,
                    1000, "2026-09-27T00:00:30Z"));
        }
        input.add(record("article-50", "10.0.0.9", "click", "on-time",
                1000, "2026-09-27T00:00:50Z"));
        input.add(record("article-51", "10.0.0.9", "click", "late-earlier",
                1000, "2026-09-27T00:00:40Z"));
        input.add(record("article-52", "10.0.0.9", "click", "late-slow",
                100_000, "2026-09-27T00:00:20Z"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        List<JSONObject> output = new ArrayList<JSONObject>();
        try (CloseableIterator<JSONObject> results = Achieve_roleC.buildRule(
                withWatermarkBefore(env, input, 51, "2026-09-27T00:02:00Z"))
                .executeAndCollect()) {
            while (results.hasNext()) {
                output.add(results.next());
            }
        }
        assertEquals(3, output.size());
        assertEquals("2026-09-27T00:00:50Z", output.get(0).getString("window_end"));
        assertEquals("2026-09-27T00:00:40Z", output.get(1).getString("window_end"));
        assertTrue(output.get(2).getBooleanValue("retracted"));
        assertEquals(0L, output.get(2).getLongValue("alert_minute")
                - java.time.Instant.parse("2026-09-27T00:00:00Z").toEpochMilli());
    }

    @Test
    public void roleBCorrectsClosedWindowAndReversesRank() throws Exception {
        List<JSONObject> input = new ArrayList<JSONObject>();
        JSONObject world1 = record("world-1", "10.0.0.1", "click", "w1", 1000,
                "2026-09-27T00:01:00Z");
        world1.put("category", "world");
        JSONObject world2 = record("world-1", "10.0.0.1", "click", "w2", 1000,
                "2026-09-27T00:02:00Z");
        world2.put("category", "world");
        JSONObject science1 = record("science-1", "10.0.0.2", "click", "s1", 1000,
                "2026-09-27T00:03:00Z");
        science1.put("category", "science");
        JSONObject advance = record("advance", "10.0.0.3", "click", "next", 1000,
                "2026-09-27T00:20:00Z");
        JSONObject science2 = record("science-1", "10.0.0.2", "click", "s2", 1000,
                "2026-09-27T00:04:00Z");
        science2.put("category", "science");
        JSONObject science3 = record("science-1", "10.0.0.2", "click", "s3", 1000,
                "2026-09-27T00:05:00Z");
        science3.put("category", "science");
        java.util.Collections.addAll(input, world1, world2, science1, advance, science2, science3);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<JSONObject> source = env.addSource(new SourceFunction<JSONObject>() {
            @Override
            public void run(SourceContext<JSONObject> context) throws Exception {
                for (int index = 0; index < input.size(); index++) {
                    if (index == 4) {
                        context.emitWatermark(new Watermark(
                                java.time.Instant.parse("2026-09-27T00:11:00Z").toEpochMilli()));
                        Thread.sleep(200);
                    }
                    JSONObject value = input.get(index);
                    context.collectWithTimestamp(value, RoleStreamUtil.eventTime(value));
                }
            }

            @Override
            public void cancel() {
            }
        });
        List<JSONObject> output = new ArrayList<JSONObject>();
        try (CloseableIterator<JSONObject> results = Achieve_roleB.buildRule(source).executeAndCollect()) {
            while (results.hasNext()) {
                output.add(results.next());
            }
        }
        List<JSONObject> firstRank = new ArrayList<JSONObject>();
        for (JSONObject result : output) {
            if ("2026-09-27T00:00:00Z".equals(result.getString("window_start"))
                    && result.getIntValue("rank") == 1) {
                firstRank.add(result);
            }
        }
        assertTrue("窗口关闭后必须发布修正结果", firstRank.size() >= 2);
        assertTrue("迟到前 world 必须一度排第一: " + firstRank,
                firstRank.stream().anyMatch(item -> "world".equals(item.getString("category"))));
        assertEquals("science", firstRank.get(firstRank.size() - 1).getString("category"));
        assertEquals(3L, firstRank.get(firstRank.size() - 1).getLongValue("score"));
        JSONObject lastSecond = null;
        for (JSONObject result : output) {
            if ("2026-09-27T00:00:00Z".equals(result.getString("window_start"))
                    && result.getIntValue("rank") == 2) {
                lastSecond = result;
            }
        }
        assertTrue(lastSecond != null);
        assertEquals("world", lastSecond.getString("category"));
        assertEquals(2L, lastSecond.getLongValue("score"));
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

    private static DataStream<JSONObject> withWatermarkBefore(
            StreamExecutionEnvironment env, List<JSONObject> values, int index, String watermarkTime) {
        return env.addSource(new SourceFunction<JSONObject>() {
            @Override
            public void run(SourceContext<JSONObject> context) throws Exception {
                for (int i = 0; i < values.size(); i++) {
                    if (i == index) {
                        context.emitWatermark(new Watermark(java.time.Instant.parse(
                                watermarkTime).toEpochMilli()));
                        Thread.sleep(100);
                    }
                    JSONObject value = values.get(i);
                    context.collectWithTimestamp(value, RoleStreamUtil.eventTime(value));
                }
            }

            @Override
            public void cancel() {
            }
        });
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

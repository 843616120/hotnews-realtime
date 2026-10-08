import com.agd.flink.join.ArticleJoinBehavior;
import com.agd.flink.roles.rolea.Achieve_roleA;
import com.agd.flink.roles.roleb.Achieve_roleB;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import model.CategoryRankingSnapshot;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.CloseableIterator;
import util.RoleStreamUtil;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 独立内存流验收，不调用生产 main，不连接 Kafka 或生产 Sink。 */
public class RulesAbAcceptance {
    public static void main(String[] args) throws Exception {
        String role = args[0];
        Path input = Paths.get(args[1]);
        Path output = Paths.get(args[2]);
        Map<String, JSONObject> articles = new HashMap<>();
        for (String line : Files.readAllLines(input.resolve("article_stream.jsonl"), StandardCharsets.UTF_8)) {
            JSONObject article = JSON.parseObject(line);
            articles.put(article.getString("article_id"), article);
        }
        List<JSONObject> behaviors = new ArrayList<>();
        for (String line : Files.readAllLines(input.resolve("behavior_stream.jsonl"), StandardCharsets.UTF_8)) {
            JSONObject value = JSON.parseObject(line);
            JSONObject article = articles.get(value.getString("article_id"));
            for (String field : new String[]{"title", "category", "tags"}) value.put(field, article.get(field));
            value.put("article_version", article.getIntValue("version"));
            behaviors.add(value);
        }
        JSONArray steps = JSON.parseArray(new String(Files.readAllBytes(input.resolve("schedule.json")),
                StandardCharsets.UTF_8));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<JSONObject> source = env.addSource(new ScheduledSource(behaviors, steps));
        // 真实使用生产去重和规则算子；富化在测试源中进行，不声称覆盖 Kafka/ETL/Join。
        SingleOutputStreamOperator<JSONObject> clean = RoleStreamUtil.prepare(
                ArticleJoinBehavior.deduplicateBehaviors(source));
        SingleOutputStreamOperator<JSONObject> result = "a".equals(role)
                ? Achieve_roleA.buildRule(clean) : Achieve_roleB.buildRule(clean);
        DataStream<JSONObject> late = result.getSideOutput("a".equals(role)
                ? Achieve_roleA.lateTag() : Achieve_roleB.lateTag());
        DataStream<String> evidence = result.map(value -> wrap("result", value))
                .union(late.map(value -> wrap("late", value)));
        if ("b".equals(role)) {
            evidence = evidence.union(result.filter(value -> value.getIntValue("rank") == 1)
                    .map(value -> {
                        CategoryRankingSnapshot snapshot = Achieve_roleB.toRedisSnapshot(value);
                        JSONObject redis = new JSONObject();
                        redis.put("key", snapshot.redisKey);
                        redis.put("window_start_ms", snapshot.windowStartMs);
                        redis.put("window_end", snapshot.windowEnd);
                        redis.put("revision", snapshot.revision);
                        redis.put("ttl_seconds", snapshot.expireSeconds);
                        redis.put("ranking", JSON.parseArray(snapshot.rankingJson));
                        return wrap("redis_snapshot", redis);
                    }));
        }
        List<String> lines = new ArrayList<>();
        try (CloseableIterator<String> iterator = evidence.executeAndCollect()) {
            while (iterator.hasNext()) lines.add(iterator.next());
        }
        Files.write(output, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        System.out.println("ROLE_" + role.toUpperCase() + " evidence=" + lines.size() + " file=" + output);
    }

    private static String wrap(String kind, JSONObject value) {
        JSONObject row = new JSONObject();
        row.put("kind", kind);
        row.put("value", value);
        return row.toJSONString();
    }

    private static class ScheduledSource implements SourceFunction<JSONObject> {
        private final List<JSONObject> behaviors;
        private final JSONArray steps;
        private volatile boolean running = true;
        ScheduledSource(List<JSONObject> behaviors, JSONArray steps) {
            this.behaviors = behaviors;
            this.steps = steps;
        }
        @Override
        public void run(SourceContext<JSONObject> context) {
            for (Object item : steps) {
                if (!running) return;
                JSONObject step = (JSONObject) item;
                synchronized (context.getCheckpointLock()) {
                    if ("watermark".equals(step.getString("kind"))) {
                        context.emitWatermark(new Watermark(step.getLongValue("watermark_ms")));
                    } else {
                        JSONObject value = behaviors.get(step.getIntValue("sequence") - 1);
                        context.collectWithTimestamp(value, RoleStreamUtil.eventTime(value));
                    }
                }
            }
        }
        @Override public void cancel() { running = false; }
    }
}

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 规则 B：计算每个十分钟窗口的分类热度前五名及分类内的热门文章。
 *
 * <p>思路：复用 Join 富化结果，对全部行为先按分类与文章在十分钟窗口内增量计数，
 * 再按同一窗口汇总分类得分并排序；迟到补算的同一文章只保留最新累计值。
 * 第二阶段使用 windowAll 汇总，不属于针对热点 Key 的加盐优化。</p>
 */
public class Achieve_roleB {
    private static final OutputTag<JSONObject> inputLateTag =
            new OutputTag<JSONObject>("role-b-input-late") {};
    private static final OutputTag<ArticleScore> lateTag =
            new OutputTag<ArticleScore>("role-b-late") {};

    public static void main(String[] args) throws Exception {
        boolean bounded = args.length == 1 && "--bounded".equals(args[0]);
        if (args.length > 0 && !bounded) {
            throw new IllegalArgumentException("仅支持 --bounded 验收参数");
        }
        //TODO 1.准备环境和检查点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(3);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        //TODO 2.接入 Join 富化后的行为流，验收模式使用独立消费组。
        SingleOutputStreamOperator<JSONObject> joined =
                ArticleJoinBehavior.createJoinedStream(env,
                        bounded ? "hotnews-role-b-verify" : "hotnews-role-b", bounded);

        //TODO 3.去重并恢复事件时间，先统计文章，再计算分类 Top 5。
        SingleOutputStreamOperator<JSONObject> result = buildRule(RoleStreamUtil.prepare(joined));
        result.print("ROLE_B");
        result.getSideOutput(lateTag).print("ROLE_B_LATE");
        env.execute("Achieve_roleB");
    }

    public static SingleOutputStreamOperator<JSONObject> buildRule(DataStream<JSONObject> joined) {
        // 第一阶段分散统计每篇文章；第二阶段全局排序，输出分类及其前五篇文章。
        SingleOutputStreamOperator<ArticleScore> articles = joined
                .keyBy(value -> value.getString("category") + "|" + value.getString("article_id"))
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(inputLateTag)
                .aggregate(new CountActions());
        articles.getSideOutput(inputLateTag).print("ROLE_B_LATE_INPUT");
        return articles.windowAll(TumblingEventTimeWindows.of(Time.minutes(10)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(lateTag)
                .process(new RankCategories());
    }

    public static class ArticleScore {
        public String articleId;
        public String category;
        public String title;
        public long count;
        public int version;
    }

    private static class CountActions implements AggregateFunction<JSONObject, ArticleScore, ArticleScore> {
        @Override
        public ArticleScore createAccumulator() {
            return new ArticleScore();
        }

        @Override
        public ArticleScore add(JSONObject value, ArticleScore score) {
            score.count++;
            score.articleId = value.getString("article_id");
            score.category = value.getString("category");
            int version = value.getIntValue("article_version");
            if (version >= score.version) {
                score.title = value.getString("title");
                score.version = version;
            }
            return score;
        }

        @Override
        public ArticleScore getResult(ArticleScore score) {
            return score;
        }

        @Override
        public ArticleScore merge(ArticleScore left, ArticleScore right) {
            left.count += right.count;
            if (right.version >= left.version) {
                left.version = right.version;
                left.title = right.title;
            }
            if (left.articleId == null) {
                left.articleId = right.articleId;
                left.category = right.category;
            }
            return left;
        }
    }

    private static class RankCategories extends ProcessAllWindowFunction<ArticleScore, JSONObject, TimeWindow> {
        @Override
        public void process(Context context, Iterable<ArticleScore> values, Collector<JSONObject> out) {
            // 迟到补算可能重复送来同一文章的累计值，按文章键保留最新累计值。
            Map<String, Map<String, ArticleScore>> grouped = new HashMap<String, Map<String, ArticleScore>>();
            for (ArticleScore score : values) {
                Map<String, ArticleScore> articles = grouped.computeIfAbsent(
                        score.category, key -> new HashMap<String, ArticleScore>());
                ArticleScore previous = articles.get(score.articleId);
                if (previous == null || score.count > previous.count) {
                    articles.put(score.articleId, score);
                }
            }
            List<String> categories = new ArrayList<String>(grouped.keySet());
            categories.sort(Comparator.<String>comparingLong(category -> -total(grouped.get(category)))
                    .thenComparing(category -> category));
            for (int rank = 0; rank < Math.min(5, categories.size()); rank++) {
                String category = categories.get(rank);
                List<ArticleScore> articles = new ArrayList<ArticleScore>(grouped.get(category).values());
                articles.sort(Comparator.<ArticleScore>comparingLong(score -> -score.count)
                        .thenComparing(score -> score.articleId));
                JSONArray topArticles = new JSONArray();
                for (int i = 0; i < Math.min(5, articles.size()); i++) {
                    ArticleScore score = articles.get(i);
                    JSONObject item = new JSONObject();
                    item.put("article_id", score.articleId);
                    item.put("title", score.title);
                    item.put("score", score.count);
                    topArticles.add(item);
                }
                JSONObject result = new JSONObject();
                result.put("rank", rank + 1);
                result.put("category", category);
                result.put("score", total(grouped.get(category)));
                result.put("top_articles", topArticles);
                result.put("window_start", Instant.ofEpochMilli(context.window().getStart()).toString());
                result.put("window_end", Instant.ofEpochMilli(context.window().getEnd()).toString());
                result.put("detect_time", Instant.now().toString());
                out.collect(result);
            }
        }

        private long total(Map<String, ArticleScore> articles) {
            long sum = 0;
            for (ArticleScore article : articles.values()) {
                sum += article.count;
            }
            return sum;
        }
    }
}

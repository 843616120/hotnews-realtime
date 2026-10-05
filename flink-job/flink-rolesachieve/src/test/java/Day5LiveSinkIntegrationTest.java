import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * 现场幂等探针：MySQL 临时表与 Redis 独立 key，不修改业务结果。
 * HOTNEWS_LIVE_MYSQL_TEST=1 / HOTNEWS_LIVE_REDIS_TEST=1 分别启用现场连接。
 */
public class Day5LiveSinkIntegrationTest {
    @Test
    public void mysqlUpsertsRejectOlderResults() throws Exception {
        assumeTrue("1".equals(System.getenv("HOTNEWS_LIVE_MYSQL_TEST")));
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://" + System.getenv().getOrDefault("MYSQL_HOST", "localhost")
                + ":" + System.getenv().getOrDefault("MYSQL_PORT", "3306")
                + "/" + System.getenv().getOrDefault("MYSQL_DATABASE", "hotnews")
                + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC";
        try (Connection db = DriverManager.getConnection(url,
                System.getenv().getOrDefault("MYSQL_USER", "root"), System.getenv("MYSQL_PASSWORD"));
             Statement setup = db.createStatement()) {
            setup.execute("CREATE TEMPORARY TABLE day5_article_probe LIKE article_alert");
            setup.execute("CREATE TEMPORARY TABLE day5_rank_probe LIKE category_rank");

            String alertSql = Day5MySqlSink.sql(Day5MySqlSink.Kind.ARTICLE_ALERT)
                    .replace("INSERT INTO article_alert(", "INSERT INTO day5_article_probe(");
            try (PreparedStatement ps = db.prepareStatement(alertSql)) {
                for (int clicks : new int[] {100, 100, 10}) {
                    JSONObject value = alert(clicks);
                    Day5MySqlSink.bind(ps, Day5MySqlSink.Kind.ARTICLE_ALERT, value);
                    ps.executeUpdate();
                }
            }
            try (ResultSet result = setup.executeQuery(
                    "SELECT COUNT(*),MAX(click_count) FROM day5_article_probe")) {
                assertTrue(result.next());
                assertEquals(1, result.getInt(1));
                assertEquals(100, result.getInt(2));
            }

            String rankSql = Day5MySqlSink.sql(Day5MySqlSink.Kind.CATEGORY_RANK)
                    .replace("INSERT INTO category_rank(", "INSERT INTO day5_rank_probe(");
            try (PreparedStatement ps = db.prepareStatement(rankSql)) {
                for (long revision : new long[] {5, 3, 6}) {
                    Day5MySqlSink.bind(ps, Day5MySqlSink.Kind.CATEGORY_RANK, rank(revision));
                    ps.executeUpdate();
                }
            }
            try (ResultSet result = setup.executeQuery(
                    "SELECT COUNT(*),MAX(revision),MAX(score) FROM day5_rank_probe")) {
                assertTrue(result.next());
                assertEquals(1, result.getInt(1));
                assertEquals(6L, result.getLong(2));
                assertEquals(600L, result.getLong(3));
            }
        }
    }

    @Test
    public void redisScriptRejectsOldWindowAndRevision() {
        assumeTrue("1".equals(System.getenv("HOTNEWS_LIVE_REDIS_TEST")));
        String key = "hotnews:day5:probe:" + UUID.randomUUID();
        try (Jedis redis = new Jedis(System.getenv().getOrDefault("REDIS_HOST", "localhost"),
                Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379")))) {
            String password = System.getenv("REDIS_PASSWORD");
            if (password != null && !password.isEmpty()) redis.auth(password);
            try {
                write(redis, key, 1000, 5, "[1,2,3,4,5]");
                assertEquals(0L, write(redis, key, 999, 9, "[]"));
                assertEquals(0L, write(redis, key, 1000, 3, "[]"));
                assertEquals("[1,2,3,4,5]", redis.hget(key, "ranking"));
                assertEquals(5L, Long.parseLong(redis.hget(key, "revision")));
                write(redis, key, 1000, 6, "[6,7,8,9,10]");
                assertEquals("[6,7,8,9,10]", redis.hget(key, "ranking"));
                assertTrue(redis.ttl(key) > 0 && redis.ttl(key) <= 7200);
            } finally {
                redis.del(key);
            }
        }
    }

    private static long write(Jedis redis, String key, long window, long revision, String ranking) {
        return (Long) redis.eval(Day5RedisRankSink.SCRIPT, Collections.singletonList(key),
                Arrays.asList(Long.toString(window), "window-end", ranking,
                        Long.toString(revision), "7200"));
    }

    private static JSONObject alert(int clicks) {
        JSONObject value = new JSONObject();
        value.put("window_start", "2026-09-27T00:00:00Z");
        value.put("window_end", "2026-09-27T00:05:00Z");
        value.put("article_id", "day5-probe");
        value.put("title", "probe");
        value.put("category", "science");
        value.put("click_count", clicks);
        value.put("detect_time", "2026-10-06T00:00:00Z");
        return value;
    }

    private static JSONObject rank(long revision) {
        JSONObject value = new JSONObject();
        value.put("window_start", "2026-09-27T00:00:00Z");
        value.put("window_end", "2026-09-27T00:10:00Z");
        value.put("rank", 1);
        value.put("category", "science");
        value.put("score", revision * 100);
        value.put("revision", revision);
        value.put("top_articles", new JSONArray());
        value.put("detect_time", "2026-10-06T00:00:00Z");
        return value;
    }
}

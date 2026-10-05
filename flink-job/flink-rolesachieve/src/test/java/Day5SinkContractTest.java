import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 第五天 Sink 的无外部服务合约测试。用法：运行 Maven test；
 * 思路：拦截 JDBC 参数绑定并检查 UPSERT，防止批次结果与撤销行错列。
 */
public class Day5SinkContractTest {
    @Test
    public void rankUpsertRejectsOldRevisionAndBindsSnapshotFields() throws Exception {
        Map<Integer, Object> parameters = new HashMap<Integer, Object>();
        JSONObject rank = new JSONObject();
        rank.put("window_start", "2026-09-27T00:00:00Z");
        rank.put("window_end", "2026-09-27T00:10:00Z");
        rank.put("rank", 1);
        rank.put("category", "science");
        rank.put("score", 5455);
        rank.put("revision", 97044);
        rank.put("top_articles", new JSONArray());
        rank.put("detect_time", "2026-10-05T10:00:00Z");
        Day5MySqlSink.bind(statement(parameters), Day5MySqlSink.Kind.CATEGORY_RANK, rank);

        assertEquals(1, parameters.get(2));
        assertEquals("science", parameters.get(4));
        assertEquals(97044L, parameters.get(8));
        assertTrue(Day5MySqlSink.sql(Day5MySqlSink.Kind.CATEGORY_RANK)
                .contains("IF(VALUES(revision)>=revision"));
    }

    @Test
    public void ipRetractionPreservesKeyAndClearsOldAlertFields() throws Exception {
        Map<Integer, Object> parameters = new HashMap<Integer, Object>();
        JSONObject retraction = new JSONObject();
        retraction.put("ip", "10.0.0.1");
        retraction.put("alert_minute", 100_000L);
        retraction.put("retracted", true);
        Day5MySqlSink.bind(statement(parameters), Day5MySqlSink.Kind.IP_ALERT, retraction);

        assertEquals(100_000L, parameters.get(1));
        assertEquals("10.0.0.1", parameters.get(2));
        assertEquals(true, parameters.get(3));
        for (int position = 4; position <= 9; position++) {
            assertTrue(parameters.containsKey(position));
            assertNull(parameters.get(position));
        }
    }

    @Test
    public void everySinkUsesUniqueKeyUpsertInsteadOfCounterIncrement() {
        for (Day5MySqlSink.Kind kind : Day5MySqlSink.Kind.values()) {
            String sql = Day5MySqlSink.sql(kind);
            assertTrue(kind.toString(), sql.contains("ON DUPLICATE KEY UPDATE"));
            assertTrue(kind.toString(), !sql.contains("click_count+1"));
        }
    }

    @Test
    public void batchSizeOverrideRejectsInvalidValues() {
        for (int invalid : new int[] {0, 10_001}) {
            try {
                new Day5MySqlSink(Day5MySqlSink.Kind.DETAIL, invalid);
                throw new AssertionError("Expected rejection for " + invalid);
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("batch size"));
            }
        }
        new Day5MySqlSink(Day5MySqlSink.Kind.DETAIL, 100);
    }

    @Test
    public void connectorDriverIsPackagedOnTheJobClasspath() throws Exception {
        assertTrue(java.sql.Driver.class.isAssignableFrom(
                Class.forName("com.mysql.cj.jdbc.Driver")));
    }

    @Test
    public void expiredRankScoreCanBeStoredWithStableExceptionKey() throws Exception {
        Achieve_roleB.ArticleScore score = new Achieve_roleB.ArticleScore();
        score.articleId = "article-000001";
        score.category = "science";
        score.windowStart = 1_000L;
        score.count = 42L;
        JSONObject event = Day5SinkJob.lateRankScore(score);
        Map<Integer, Object> parameters = new HashMap<Integer, Object>();
        Day5MySqlSink.bind(statement(parameters), Day5MySqlSink.Kind.PIPELINE_EVENT, event);
        assertEquals("ROLE_B_LATE", parameters.get(1));
        assertEquals("article-000001-1000", parameters.get(2));
        assertEquals("RANK_STATE_EXPIRED", parameters.get(4));
        assertTrue(((String) parameters.get(5)).contains("\"score\":42"));
    }

    /** 测试替身只记录 PreparedStatement 的 setXxx 参数，不打开数据库连接。 */
    private static PreparedStatement statement(Map<Integer, Object> parameters) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] { PreparedStatement.class },
                (proxy, method, args) -> {
                    if (method.getName().startsWith("set")) {
                        parameters.put((Integer) args[0],
                                "setNull".equals(method.getName()) ? null : args[1]);
                    }
                    return null;
                });
    }
}

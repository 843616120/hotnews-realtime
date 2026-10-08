import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Instant;

/** 只读取分类结果和最新榜单，不读取 Kafka，不执行任何数据库写入。 */
public class RankingRead {
    public static void main(String[] args) throws Exception {
        JSONObject report = new JSONObject();
        report.put("captured_at", Instant.now().toString());
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection connection = DriverManager.getConnection(
                "jdbc:mysql://localhost:3307/hotnews?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC",
                "root", "root")) {
            connection.setReadOnly(true);
            try (Statement statement = connection.createStatement();
                 ResultSet data = statement.executeQuery(
                         "SELECT * FROM category_rank ORDER BY window_start_ms,rank_no")) {
                JSONArray rows = new JSONArray();
                ResultSetMetaData columns = data.getMetaData();
                while (data.next()) {
                    JSONObject row = new JSONObject();
                    for (int i = 1; i <= columns.getColumnCount(); i++) {
                        row.put(columns.getColumnLabel(i), data.getObject(i));
                    }
                    rows.add(row);
                }
                report.put("mysql_rows", rows);
                report.put("mysql_read_at", Instant.now().toString());
            }
        }
        try (Jedis redis = new Jedis("localhost", 6379)) {
            JSONObject value = new JSONObject();
            value.put("key", "hotnews:top5:latest");
            value.put("type", redis.type("hotnews:top5:latest"));
            value.put("value", redis.hgetAll("hotnews:top5:latest"));
            value.put("ttl_seconds", redis.ttl("hotnews:top5:latest"));
            value.put("read_at", Instant.now().toString());
            report.put("redis", value);
        }
        Files.write(Paths.get(args[0]), report.toJSONString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE_NEW);
        System.out.println("Only-read ranking evidence: " + args[0]);
    }
}

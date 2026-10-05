import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;

/**
 * Redis 最新榜单 Sink。用法：接收规则 B 的每条 rank 结果，查询 hotnews:top5:latest；
 * 思路：只接收 rank=1 携带的完整 Top 5 快照；Lua 比较窗口起点与单调修订号，
 * 原子覆盖全部榜单并保留 2 小时，避免不同修订版的 rank 混合。
 */
public class Day5RedisRankSink extends RichSinkFunction<JSONObject> {
    private static final String KEY = "hotnews:top5:latest";
    static final String SCRIPT =
            "local previous=tonumber(redis.call('HGET',KEYS[1],'window_start_ms') or '-1') "
            + "local incoming=tonumber(ARGV[1]) "
            + "if incoming < previous then return 0 end "
            + "local previousRevision=tonumber(redis.call('HGET',KEYS[1],'revision') or '-1') "
            + "local revision=tonumber(ARGV[4]) "
            + "if incoming == previous and revision < previousRevision then return 0 end "
            + "if incoming > previous then redis.call('DEL',KEYS[1]) end "
            + "redis.call('HSET',KEYS[1],'window_start_ms',ARGV[1],"
            + "'window_end',ARGV[2],'ranking',ARGV[3],'revision',ARGV[4]) "
            + "redis.call('EXPIRE',KEYS[1],ARGV[5]) return 1";
    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) {
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        jedis = new Jedis(host, port);
        String password = System.getenv("REDIS_PASSWORD");
        if (password != null && !password.isEmpty()) jedis.auth(password);
        jedis.ping();
    }

    @Override
    public void invoke(JSONObject value, Context context) {
        long start = OffsetDateTime.parse(value.getString("window_start")).toInstant().toEpochMilli();
        jedis.eval(SCRIPT, Collections.singletonList(KEY), Arrays.asList(
                Long.toString(start), value.getString("window_end"),
                value.getJSONArray("ranking").toJSONString(),
                value.getString("revision"), "7200"));
    }

    @Override
    public void close() {
        if (jedis != null) jedis.close();
    }
}

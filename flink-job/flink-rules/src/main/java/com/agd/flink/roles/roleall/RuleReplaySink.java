package com.agd.flink.roles.roleall;

import com.agd.flink.roles.rolea.Achieve_roleA;
import com.agd.flink.roles.roleb.Achieve_roleB;
import com.agd.flink.roles.rolec.Achieve_roleC;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import util.FlinkSinkUtil;
import util.RoleStreamUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/** 超期窗口从完整清洗表重算；同一个作业内执行，不另起 Kafka 消费程序。 */
public class RuleReplaySink extends RichSinkFunction<JSONObject>
        implements CheckpointedFunction, CheckpointListener {
    private transient ListState<String> saved;
    private transient ListState<String> savedC;
    private transient Set<String> requests;
    private transient Set<String> replayC;
    private transient TreeMap<Long, Set<String>> checkpoints;
    private transient Connection mysql;
    private transient Jedis redis;

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        saved = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<>("rule-replay-requests", String.class));
        savedC = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<>("rule-replay-c-minutes", String.class));
        requests = new HashSet<>();
        replayC = new HashSet<>();
        checkpoints = new TreeMap<>();
        for (String key : saved.get()) requests.add(key);
        for (String key : savedC.get()) replayC.add(key);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mysql = FlinkSinkUtil.connectMySql();
        redis = FlinkSinkUtil.connectRedis();
    }

    @Override
    public void invoke(JSONObject value, Context ctx) {
        long time = RoleStreamUtil.eventTime(value);
        long watermark = ctx.currentWatermark();
        long minute = Math.floorDiv(time, 60000) * 60000;
        long bStart = Math.floorDiv(time, 600000) * 600000;
        String type = value.getString("replay_rule");
        if (type != null) {
            // 旁路已由实际规则判定为迟到，不依赖本 Sink 的另一份 Watermark。
            if ("B".equals(type)) requests.add("B|" + bStart);
            if ("A".equals(type)) {
                for (int offset = 0; offset < 5; offset++) {
                    requests.add("A|" + (minute - offset * 60000) + "|" + value.getString("article_id"));
                }
            }
            if ("C".equals(type)) {
                for (int offset = 0; offset < 2; offset++) {
                    String key = "C|" + (minute + offset * 60000) + "|" + value.getString("ip");
                    requests.add(key);
                    replayC.add(key);
                }
            }
            return;
        }
        if (watermark >= bStart + 600000 + 3900000) requests.add("B|" + bStart);
        if (!"click".equals(value.getString("action"))) return;
        for (int offset = 0; offset < 5; offset++) {
            long start = minute - offset * 60000;
            if (watermark >= start + 300000 + 3900000) {
                requests.add("A|" + start + "|" + value.getString("article_id"));
            }
        }
        for (int offset = 0; offset < 2; offset++) {
            long target = minute + offset * 60000;
            // 一个回看过期，另一个可能也缺失已清理的前一分钟点击，两个都完整重算。
            String key = "C|" + target + "|" + value.getString("ip");
            if (watermark >= minute + 60000 + 3900000 || replayC.contains(key)) {
                requests.add(key);
                replayC.add(key);
            }
        }
        // 已完整过期的分钟以后每条补发都会走超期条件，不再需要额外记录所有权。
        replayC.removeIf(key -> watermark >= Long.parseLong(key.split("\\|")[1]) + 60000 + 3900000);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        checkpoints.put(ctx.getCheckpointId(), new HashSet<>(requests));
        requests.clear();
        Set<String> pending = new HashSet<>();
        for (Set<String> keys : checkpoints.values()) pending.addAll(keys);
        saved.update(new ArrayList<>(pending));
        savedC.update(new ArrayList<>(replayC));
    }

    @Override
    public void notifyCheckpointComplete(long id) throws Exception {
        Set<String> ready = new HashSet<>();
        for (Set<String> keys : checkpoints.headMap(id, true).values()) ready.addAll(keys);
        // 全局 CK 成功意味着所有 clean_behavior Sink 已提交。用完整窗口重算，
        // 避免另一个 Sink 的批次尚未 flush 时查询少数据，也不把 late 部分当成总量。
        for (String key : ready) replay(key);
        // snapshot 后同一窗口还可能收到新数据，不能把新的请求一起删除。
        checkpoints.headMap(id, true).clear();
        getRuntimeContext().getMetricGroup().counter("replayed_windows").inc(ready.size());
    }

    private void replay(String key) throws Exception {
        String[] parts = key.split("\\|");
        long start = Long.parseLong(parts[1]);
        if ("A".equals(parts[0])) {
            Achieve_roleA.CountClicks counter = new Achieve_roleA.CountClicks();
            Achieve_roleA.ClickAccumulator total = counter.createAccumulator();
            for (JSONObject row : read(start, start + 300000, "article_id", parts[2])) counter.add(row, total);
            JSONObject result = Achieve_roleA.result(parts[2], start, total);
            if (result != null) write(Achieve_roleA.MYSQL_SQL, Achieve_roleA::bindMySql, result);
            if (result != null) System.out.println("ROLE_A> " + result.toJSONString());
        } else if ("B".equals(parts[0])) {
            Achieve_roleB.CountActions counter = new Achieve_roleB.CountActions();
            Achieve_roleB.RankingAccumulator total = counter.createAccumulator();
            for (JSONObject row : read(start, start + 600000, null, null)) counter.add(row, total);
            List<JSONObject> ranking = Achieve_roleB.ranking(start, total);
            for (JSONObject row : ranking) {
                write(Achieve_roleB.MYSQL_SQL, Achieve_roleB::bindMySql, row);
                System.out.println("ROLE_B> " + row.toJSONString());
            }
            if (!ranking.isEmpty()) Achieve_roleB.writeRedisRanking(redis, Achieve_roleB.toRedisSnapshot(ranking.get(0)));
        } else {
            List<JSONObject> rows = read(start - 60000 + 1, start + 60000, "ip", parts[2]);
            Achieve_roleC.IpWindowState result = Achieve_roleC.evaluate(start, rows);
            if (result == null) return;
            JSONObject row = new JSONObject();
            row.put("alert_minute", start);
            row.put("ip", parts[2]);
            row.put("retracted", !result.alert);
            // 完整清洗量单调增加，用它给补算版本排序；在线普通修订号小于此版本。
            row.put("revision", 1000000000000L + rows.size());
            row.put("window_start", Instant.ofEpochMilli(result.start).toString());
            row.put("window_end", Instant.ofEpochMilli(result.end).toString());
            row.put("article_count", result.articleIds.size());
            row.put("click_count", result.clickCount);
            row.put("read_duration_sum_ms", result.durationSum);
            row.put("avg_read_duration_ms", (double) result.durationSum / result.clickCount);
            row.put("article_ids", result.articleIds);
            row.put("detect_time", Instant.now().toString());
            write(Achieve_roleC.MYSQL_SQL, Achieve_roleC::bindMySql, row);
            System.out.println("ROLE_C> " + row.toJSONString());
        }
    }

    private List<JSONObject> read(long from, long to, String field, String key) throws Exception {
        String sql = "SELECT event_id,article_id,action,ip,event_time,read_duration_ms,title,category,article_version "
                + "FROM clean_behavior WHERE event_time_ms>=? AND event_time_ms<?"
                + (field == null ? "" : " AND action='click' AND " + field + "=?");
        List<JSONObject> rows = new ArrayList<>();
        try (PreparedStatement ps = mysql.prepareStatement(sql)) {
            ps.setLong(1, from);
            ps.setLong(2, to);
            if (field != null) ps.setString(3, key);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JSONObject row = new JSONObject();
                    for (String name : new String[]{"event_id", "article_id", "action", "ip",
                            "event_time", "title", "category"}) row.put(name, rs.getString(name));
                    row.put("read_duration_ms", rs.getLong("read_duration_ms"));
                    row.put("article_version", rs.getInt("article_version"));
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private void write(String sql, FlinkSinkUtil.JdbcBinder<JSONObject> binder, JSONObject row) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(sql)) {
            binder.bind(ps, row);
            ps.executeUpdate();
        }
    }

    @Override
    public void close() throws Exception {
        if (mysql != null) mysql.close();
        if (redis != null) redis.close();
    }
}

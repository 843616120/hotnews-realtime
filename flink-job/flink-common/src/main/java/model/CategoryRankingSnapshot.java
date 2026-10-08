package model;

import java.io.Serializable;

/**
 * Redis 榜单输出 Bean。
 *
 * <p>这个类只描述规则 B 要写入 Redis 的完整快照，不持有 Jedis 连接，
 * 也不负责执行 Lua。这样规则 B 可以明确产生什么数据，公共 Redis Sink
 * 只负责把 Bean 交给业务 Writer。</p>
 */
public class CategoryRankingSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 业务 Redis Hash 的固定 Key。 */
    public String redisKey;
    /** 榜单对应窗口的起点毫秒值。 */
    public long windowStartMs;
    /** 榜单对应窗口的 ISO-8601 结束时间。 */
    public String windowEnd;
    /** 同窗口内单调递增的业务修订号。 */
    public long revision;
    /** 完整 Top 5 快照的 JSON 文本。 */
    public String rankingJson;
    /** Redis 过期秒数。 */
    public int expireSeconds;

    public CategoryRankingSnapshot() {
        // Flink POJO 和状态序列化需要无参构造函数。
    }

    public CategoryRankingSnapshot(String redisKey, long windowStartMs, String windowEnd,
                                   long revision, String rankingJson, int expireSeconds) {
        this.redisKey = redisKey;
        this.windowStartMs = windowStartMs;
        this.windowEnd = windowEnd;
        this.revision = revision;
        this.rankingJson = rankingJson;
        this.expireSeconds = expireSeconds;
    }
}

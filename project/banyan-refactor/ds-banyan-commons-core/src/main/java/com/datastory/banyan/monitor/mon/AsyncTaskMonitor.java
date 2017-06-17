package com.datastory.banyan.monitor.mon;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.redis.RedisConsts;
import com.datastory.banyan.redis.RedisDao;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.FreqDist;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.monitor.mon.AsyncTaskMonitor
 *
 * @author lhfcws
 * @since 2017/4/24
 */
public class AsyncTaskMonitor {

    protected static Logger LOG = Logger.getLogger(RedisMetricMonitor.class);

    private static volatile AsyncTaskMonitor _singleton = null;

    public static AsyncTaskMonitor getInstance() {
        if (_singleton == null) {
            synchronized (AsyncTaskMonitor.class) {
                if (_singleton == null) {
                    _singleton = new AsyncTaskMonitor();
                }
            }
        }
        return _singleton;
    }

    protected RedisDao dao = RedisDao.getInstance(RhinoETLConfig.getInstance());
    protected String prefixKey = RedisConsts.TASK_MON_KEY;

    protected AsyncTaskMonitor() {

    }

    /**
     * 未来可能考虑加入按数据源做分别
     *
     * @return
     */
    protected String getKey(String taskId) {
        return RedisConsts.TASK_MON_KEY + taskId;
    }

    public void inc(FreqDist<String> dist) {
        if (dist == null || dist.isEmpty())
            return;

        long now = System.currentTimeMillis();
        int retry = 3;
        Exception exc = null;

        List<Pair<String, Long>> list = new ArrayList<>();
        for (Map.Entry<String, Integer> e : dist.entrySet()) {
            list.add(new Pair<>(getKey(e.getKey()), e.getValue().longValue()));
        }
        JedisPool jedisPool = dao.getJedisPool(prefixKey);
        Jedis jedis = null;

        while (retry-- >= 0) {
            try {
                jedis = jedisPool.getResource();
                Pipeline pipeline = jedis.pipelined();
                for (Pair<String, Long> pair : list) {
                    pipeline.hincrBy(pair.getFirst(), "data", pair.getSecond());
                    pipeline.hset(pair.getFirst(), "wts", now + "");
                    pipeline.expire(pair.getFirst(), 86400 * 5);    // 默认最后一次更新后保存五天
                }
                pipeline.sync();
                jedisPool.returnResource(jedis);
                exc = null;
                break;
            } catch (Exception e) {
                exc = e;
                if (jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
        if (exc != null)
            ErrorUtil.error(LOG, exc);
    }

    public Long get(String taskId) {
        int retry = 3;
        Exception exc = null;
        while (retry-- >= 0) {
            try {
                String value = dao.hget(getKey(taskId), "data");
                exc = null;
                if (value != null)
                    return Long.valueOf(value);
                else
                    return null;
            } catch (Exception e) {
                exc = e;
            }
        }

        if (exc != null) {
            ErrorUtil.error(LOG, exc);
            throw new RuntimeException(exc);
        } else
            return null;
    }

    /**
     * 爬虫组给代码逻辑，如何通过一个taskId或多余的字段判断当前taskId是否需要记录progress。
     *
     * @param jsonObject
     * @return
     */
    public static boolean needRecordProgress(JSONObject jsonObject) {
        if (jsonObject == null)
            return false;

        try {
            if (jsonObject.containsKey("_track_count_"))
                return jsonObject.getBoolean("_track_count_");
            else
                return false;
        } catch (Exception e) {
            return false;
        }
    }

    public static String getTaskId(JSONObject jsonObject) {
        return jsonObject.getString("jobName");
    }

    public static void main(String[] args) throws Exception {
        FreqDist<String> dist = new FreqDist<>();
        AsyncTaskMonitor mon = AsyncTaskMonitor.getInstance();
        dist.inc("t");
        dist.inc("t2");
        dist.inc("t2");
        System.out.println(dist);
        mon.inc(dist);
    }
}

package com.datastory.banyan.monitor.mon;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.monitor.utils.QuartKeyCreator;
import com.datastory.banyan.redis.RedisConsts;
import com.datastory.banyan.redis.RedisDao;
import com.datastory.banyan.utils.ErrorUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * com.datastory.banyan.monitor.mon.RedisMetricMonitor
 * <p>
 * Record metrcis by day
 *
 * @author lhfcws
 * @since 2016/11/4
 */
public class RedisMetricMonitor implements MetricMonitor {
    protected static Logger LOG = Logger.getLogger(RedisMetricMonitor.class);

    private static volatile RedisMetricMonitor _singleton = null;

    public static RedisMetricMonitor getInstance() {
        if (_singleton == null) {
            synchronized (RedisMetricMonitor.class) {
                if (_singleton == null) {
                    _singleton = new RedisMetricMonitor();
                }
            }
        }
        return _singleton;
    }

    protected RedisDao dao = RedisDao.getInstance(RhinoETLConfig.getInstance());
    protected QuartKeyCreator quartKeyCreator = new QuartKeyCreator(dao);
    protected String prefixKey = RedisConsts.METRICS_MON_KEY;

    protected RedisMetricMonitor() {
        quartKeyCreator.start();
    }

    protected String getKey() {
        return QuartKeyCreator.key(prefixKey);
    }

    @Override
    public void inc(String key) {
        inc(key, 1);
    }

    @Override
    public synchronized void inc(String key, long value) {
        int retry = 3;
        Exception exc = null;
        while (retry-- >= 0) {
            try {
                dao.hincrBy(getKey(), key, value);
                exc = null;
            } catch (Exception e) {
                exc = e;
            }
        }
        if (exc != null)
            ErrorUtil.error(LOG, exc);
    }

    public synchronized void inc(List<Pair<String, Long>> list) {
        if (list == null || list.isEmpty())
            return;

        int retry = 3;
        Exception exc = null;
        while (retry-- >= 0) {
            try {
                dao.hincrByPipeline(getKey(), list);
                exc = null;
            } catch (Exception e) {
                exc = e;
            }
        }
        if (exc != null)
            ErrorUtil.error(LOG, exc);
    }

    @Override
    public void set(String key, Object value) {
        int retry = 3;
        Exception exc = null;
        while (retry-- >= 0) {
            try {
                dao.hset(getKey(), key, String.valueOf(value));
                exc = null;
            } catch (Exception e) {
                exc = e;
            }
        }
        if (exc != null)
            ErrorUtil.error(LOG, exc);
    }

    @Override
    public Long get(String updateDate, String key) {
        String mainKey = prefixKey + updateDate;
        try {
            String res = dao.hget(mainKey, key);
            return Long.valueOf(res);
        } catch (Exception e) {
            ErrorUtil.error(LOG, e);
            return null;
        }
    }
}

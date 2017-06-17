package com.datastory.banyan.async;


import com.datastory.banyan.base.RhinoETLConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author lhfcws
 * @since 16/1/12.
 */
public class AsyncPool {
    protected ExecutorService executorService;
    protected static Configuration conf = RhinoETLConfig.getInstance();

    public AsyncPool() {
        int size = conf.getInt("banyan.async.pool.size.default", 100);
        executorService = Executors.newScheduledThreadPool(size);
    }

    public AsyncPool(int size) {
        executorService = Executors.newScheduledThreadPool(size);
    }

    public ExecutorService getPool() {
        return executorService;
    }

    // 常驻监控函数池
    public static final AsyncPool MONITOR_POOL = new AsyncPool(conf.getInt("banyan.async.pool.size.monitor", 100));
    public static final AsyncPool RUN_POOL = new AsyncPool(conf.getInt("banyan.async.pool.size.run", 100));

}

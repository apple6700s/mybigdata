package com.datastory.banyan.monitor.utils;

import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.monitor.mon.RedisMetricMonitor;
import com.datastory.banyan.monitor.stat.MonStat;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * com.datastory.banyan.monitor.utils.MonUtil
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class MonUtil {
    public static void store(String key, MonStat monStat) {
        List<Pair<String, Long>> list = new ArrayList<>();
        if (monStat.getRetry() > 0)
            list.add(new Pair<>(MonConsts.keys(key, MonConsts.RETRY), monStat.getRetry()));
        if (monStat.getFail() > 0)
            list.add(new Pair<>(MonConsts.keys(key, MonConsts.FAIL), monStat.getFail()));
        if (monStat.getSuccess() > 0)
            list.add(new Pair<>(MonConsts.keys(key, MonConsts.SUCCESS), monStat.getSuccess()));
        if (monStat.getTotal() > 0)
            list.add(new Pair<>(MonConsts.keys(key, MonConsts.TOTAL), monStat.getTotal()));
        if (!list.isEmpty())
            RedisMetricMonitor.getInstance().inc(list);
    }
}

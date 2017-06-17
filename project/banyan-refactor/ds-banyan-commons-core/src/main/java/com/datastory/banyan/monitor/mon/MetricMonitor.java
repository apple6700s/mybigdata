package com.datastory.banyan.monitor.mon;

/**
 * com.datatub.rhino.monitor.mon.MetricMonitor
 *
 * @author lhfcws
 * @since 2016/11/4
 */
public interface MetricMonitor {
    public void inc(String key);
    public void inc(String key, long value);
    public void set(String key, Object value);
    public Long get(String updateDate, String key);
}

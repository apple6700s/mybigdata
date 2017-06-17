package com.datastory.banyan.monitor.reporter;


import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.monitor.mon.FalconMetricMonitor;

/**
 * com.datastory.banyan.monitor.reporter.AliveFalconMetricReporter
 *
 * @author lhfcws
 * @since 2016/11/7
 */
public class AliveFalconMetricReporter extends AbstractReporter {
    private String name;
    public AliveFalconMetricReporter(String name) {
        this.name = name;
        String key = MonConsts.aliveKey(getName());
        FalconMetricMonitor.getInstance().register(key);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void report() {
        String key = MonConsts.aliveKey(getName());
        FalconMetricMonitor.getInstance().set(key, "1");
    }

    @Override
    public String getCron() {
        return "*/1 * * * * ?";
    }

    @Override
    public void init() {

    }
}

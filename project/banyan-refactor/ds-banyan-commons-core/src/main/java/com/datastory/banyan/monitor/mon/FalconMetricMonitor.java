package com.datastory.banyan.monitor.mon;

import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.AdvRunnable;
import com.yeezhao.commons.util.openfalcon.FalconCounter;
import com.yeezhao.commons.util.openfalcon.MetricCenter;

import java.util.Random;

/**
 * com.datatub.rhino.monitor.mon.FalconMetricMonitor
 *
 * @author lhfcws
 * @since 2016/11/7
 */
@Deprecated
public class FalconMetricMonitor implements MetricMonitor {
    private static volatile FalconMetricMonitor _singleton = null;

    public static FalconMetricMonitor getInstance() {
        if (_singleton == null) {
            synchronized (FalconMetricMonitor.class) {
                if (_singleton == null) {
                    _singleton = new FalconMetricMonitor();
                }
            }
        }
        return _singleton;
    }

    private MetricCenter metricCenter;
    private FalconCounter counter;

    private FalconMetricMonitor() {
        metricCenter = new MetricCenter();
        metricCenter.setHost("banyan");
//        metricCenter.setUrl("http://dev4:1988/v1/push");
        metricCenter.setReportInterval(5);
        metricCenter.getFalconCounter().namespace(MonConsts.OPS_GRP, MonConsts.OPS_PROJ);
        counter = metricCenter.getFalconCounter();
        metricCenter.start();
    }

    public void register(String key, int intervalSecond) {
        this.metricCenter.registerMetric(key, intervalSecond, false);
    }

    public void register(String key) {
        register(key, 1);
    }

    @Override
    public void inc(String key) {
        inc(key, 1);
    }

    @Override
    public void inc(String key, long value) {
        counter.counter.setDefault(key);
        counter.inc(key, value);
    }

    @Override
    public void set(String key, Object value) {
        try {
            counter.counter.setDefault(key);
            counter.set(key, BanyanTypeUtil.parseInt(value));
        } catch (Exception ignore) {}
    }

    @Override
    public Long get(String updateDate, String key) {
        return null;
    }

    public void avg(String key, double value) {
        counter.counter.setDefault(key);
        counter.avg(key, value);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        final String metric = MonConsts.keys(MonConsts.M_HB_OUT, MonConsts.MON_WBUSER);
//        final MetricCenter metricCenter = new MetricCenter();
//        metricCenter.setHost("banyantest");
//        metricCenter.setUrl("http://dev4:1988/v1/push");
//        metricCenter.registerMetric(metric, 1);
//        metricCenter.setReportInterval(5);
//        metricCenter.start();
        final Random random = new Random();
        new AdvRunnable(""){
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    int r = random.nextInt(1000);
                    FalconMetricMonitor.getInstance().register(metric);
                    FalconMetricMonitor.getInstance().avg(metric, r);
                }
            }
        }.start();

        Thread.currentThread().join();
        System.out.println("[PROGRAM] Program exited.");
    }
}

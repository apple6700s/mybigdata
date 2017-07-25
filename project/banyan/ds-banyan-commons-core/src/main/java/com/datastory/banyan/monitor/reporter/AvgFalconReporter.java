package com.datastory.banyan.monitor.reporter;


import com.datastory.banyan.monitor.MonConsts;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.openfalcon.FalconReporter;
import com.yeezhao.commons.util.openfalcon.OpenFalconMetric;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.quartz.SchedulerException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.monitor.reporter.SumFalconReporter
 *
 * @author lhfcws
 */
public class AvgFalconReporter extends FalconReporter implements QuartzExecutor {
    List<OpenFalconMetric> metrics = new LinkedList<>();

    public AvgFalconReporter() {
        setHost("banyan");
        namespace(MonConsts.OPS_GRP, MonConsts.OPS_PROJ);
        try {
            start();
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    private static volatile AvgFalconReporter _singleton = null;

    public static AvgFalconReporter getInstance() {
        if (_singleton == null) {
            synchronized (AvgFalconReporter.class) {
                if (_singleton == null) {
                    _singleton = new AvgFalconReporter();
                }
            }
        }
        return _singleton;
    }

    public void start() throws SchedulerException {
        QuartzJobUtils.createQuartzJob(QuartzJobUtils.CRON_HALF_MINUTE, this.getClass().getSimpleName(), this);
    }

    public void addMetric(String key, double value) {
        OpenFalconMetric metric = toMetric(key, value);
        synchronized (this) {
            metrics.add(metric);
        }
    }

    public void report() {
        List<OpenFalconMetric> list = null;
        synchronized (this) {
            list = new ArrayList<>(metrics);
            metrics.clear();
        }
        if (list.isEmpty())
            return;

        HashMap<Long, OpenFalconMetric> mp = new HashMap<>();
        FreqDist<Long> freq = new FreqDist<>();
        for (OpenFalconMetric m : list) {
            long secondTS = m.getTimestamp();
            if (mp.containsKey(secondTS)) {
                OpenFalconMetric metric = mp.get(secondTS);
                freq.inc(secondTS);
                metric.setValue(metric.getValue() + m.getValue());
            } else
                mp.put(secondTS, m);
        }

        for (OpenFalconMetric m : mp.values()) {
            Integer num = freq.get(m.getTimestamp());
            if (num == null || num == 0) num = 1;
            m.setValue(m.getValue() / num);
        }

        if (!mp.isEmpty())
            report2LocalFalconServer(new LinkedList<>(mp.values()));
    }

    @Override
    public void execute() {
        report();
    }
}

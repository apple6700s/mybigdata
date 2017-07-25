package com.datastory.banyan.monitor.reporter;

import com.datastory.banyan.monitor.MonConsts;
import com.yeezhao.commons.util.openfalcon.OpenFalconMetric;
import com.yeezhao.commons.util.openfalcon.FalconReporter;
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
public class SumFalconReporter extends FalconReporter implements QuartzExecutor {
    List<OpenFalconMetric> metrics = new LinkedList<>();

    public SumFalconReporter() {
        setHost("banyan");
        namespace(MonConsts.OPS_GRP, MonConsts.OPS_PROJ);
        try {
            start();
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    private static volatile SumFalconReporter _singleton = null;

    public static SumFalconReporter getInstance() {
        if (_singleton == null) {
            synchronized (SumFalconReporter.class) {
                if (_singleton == null) {
                    _singleton = new SumFalconReporter();
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
        for (OpenFalconMetric m : list) {
            long secondTS = m.getTimestamp();
            if (mp.containsKey(secondTS)) {
                OpenFalconMetric metric = mp.get(secondTS);
                metric.setValue(metric.getValue() + m.getValue());
            } else
                mp.put(secondTS, m);
        }

        if (!mp.isEmpty())
            report2LocalFalconServer(new LinkedList<>(mp.values()));
    }

    @Override
    public void execute() {
        report();
    }
}

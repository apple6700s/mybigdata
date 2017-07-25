package com.datastory.banyan.monitor.reporter;

import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;

/**
 * com.datatub.rhino.monitor.reporter.AbstractReporter
 *
 * @author lhfcws
 * @since 2016/11/7
 */
public abstract class AbstractReporter implements IReporter {
    private JobDetail jobDetail = null;

    @Override
    public String getName() {
        return this.getClass().getCanonicalName();
    }

    @Override
    public void start() throws Exception {
        stop();
        init();
        QuartzJobUtils.createQuartzJob(getCron(), getName(), this);
    }

    @Override
    public void stop() {
        if (jobDetail != null) {
            try {
                QuartzJobUtils.removeQuartzJob(jobDetail);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }
    }

    public abstract void init();

    @Override
    public void execute() {
        report();
    }
}

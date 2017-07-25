package com.datastory.banyan.monitor.reporter;

import com.yeezhao.commons.util.quartz.QuartzExecutor;

import java.io.Serializable;

/**
 * com.datatub.rhino.monitor.reporter.IReporter
 *
 * @author lhfcws
 * @since 2016/11/7
 */
public interface IReporter extends Serializable, QuartzExecutor {
    public String getName();
    public void start() throws Exception;
    public void stop();
    public void report();
    public String getCron();
}

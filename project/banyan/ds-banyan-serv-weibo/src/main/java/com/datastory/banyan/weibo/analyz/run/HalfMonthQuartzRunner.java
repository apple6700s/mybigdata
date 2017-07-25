package com.datastory.banyan.weibo.analyz.run;

import com.datastory.banyan.weibo.analyz.component.HalfMonthComponentAnalyzer;
import com.datastory.banyan.weibo.analyz.component.UTypeComponentAnalyzer;
import com.datastory.banyan.weibo.analyz.user_component.UTypeUserAnalyzer;
import com.datastory.banyan.weibo.spark.ScanContentUidSparkAnalyzer;
import com.datastory.banyan.weibo.spark.ScanUserSparkAnalyzer;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Date;

/**
 * com.datastory.banyan.weibo.analyz.run.HalfMonthQuartzRunner
 *
 * @author lhfcws
 * @since 2017/1/13
 */
public class HalfMonthQuartzRunner implements Serializable, QuartzExecutor {
    public static final String CRON_HALFMONTH = "0 0 0 1,15 * ?";

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        QuartzJobUtils.createQuartzJob(CRON_HALFMONTH, "HalfMonthQuartzRunner", new HalfMonthQuartzRunner());
        System.out.println("[PROGRAM] Program exited. " + new Date());
    }

    @Override
    public void execute() {
        // analyz HalfMonthComponentAnalyzer
        try {
            ScanContentUidSparkAnalyzer.main(new String[]{
                    HalfMonthComponentAnalyzer.class.getCanonicalName()
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        // analyz utype
        try {
            ScanUserSparkAnalyzer.main(new String[]{
                    UTypeUserAnalyzer.class.getCanonicalName()
            });
            ScanContentUidSparkAnalyzer.main(new String[]{
                    UTypeComponentAnalyzer.class.getCanonicalName()
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

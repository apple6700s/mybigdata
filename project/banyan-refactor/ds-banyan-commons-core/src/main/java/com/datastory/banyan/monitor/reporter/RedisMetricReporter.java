package com.datastory.banyan.monitor.reporter;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.monitor.checker.PercentChecker;
import com.datastory.banyan.monitor.utils.QuartKeyCreator;
import com.datastory.banyan.redis.RedisDao;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.utils.Emailer;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.FreqDist;
import org.apache.hadoop.conf.Configuration;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * com.datastory.banyan.monitor.reporter.RedisMetricReporter
 * Report metrics an hour before
 *
 * @author lhfcws
 * @since 2016/11/7
 */
public class RedisMetricReporter extends AbstractReporter {
    public static boolean testMode = false;
    private RedisDao dao;

    @Override
    public void init() {
        Configuration conf = RhinoETLConfig.getInstance();
        dao = RedisDao.getInstance(conf);
    }

    @Override
    public void report() {
//        Calendar calendar = Calendar.getInstance();
//        calendar.add(Calendar.HOUR_OF_DAY, -1);
        String monKey = QuartKeyCreator.key();
        System.out.println(monKey);
        try {
            _report(monKey);
        } catch (Exception e) {
            ErrorUtil.error(e);
        }
    }

    @Override
    public String getCron() {
        return "0 59 * * * ?";
    }

    public void _report(String monKey) throws Exception {
        boolean needDailyReport = isLatestOfDay();

        Map<String, String> mp = dao.hgetAll(monKey);
        mp.remove("_0");
        List<String> keys = BanyanTypeUtil.sortStrMapKeys(mp);
        PercentChecker percentChecker = new PercentChecker();
        percentChecker.setThreshold(0.05);

        String title = null;
        double maxErrorRate = 0;
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            if (key.endsWith(MonConsts.SUCCESS)) {
                i++;
                String totalKey = keys.get(i);
                double errorRate = percentChecker.calcRate(mp.get(totalKey), mp.get(key));
                if (maxErrorRate < errorRate)
                    maxErrorRate = errorRate;
            }
        }

        if (maxErrorRate > 0) {
            maxErrorRate *= 100;
            title = String.format("最高 %.2f %s 入库失败量[当前小时]", maxErrorRate, "%");
            String data = BanyanTypeUtil.prettySortStringifyMap(mp);
            data = monKey + " ==> \n" + data;
            Emailer.sendMonitorEmail(title, data);
        } else {
            System.out.println(DateUtils.getCurrentTimeStr() + " " + mp + " 全部成功，不需发邮件");
        }

        // SEND DAILY REPORT
        if (needDailyReport) {
            sendDailyReport(monKey.substring(0, monKey.length() - 2));
        }
    }

    private void sendDailyReport(String datePrefix) throws Exception {
        Set<String> allKeys = dao.keys(datePrefix + "*");
        System.out.println("DatePrefix: " + datePrefix + " , " + allKeys);
        FreqDist<String> freqDist = new FreqDist<>();
        for (String key : allKeys) {
            Map<String, String> mp = dao.hgetAll(key);
            mp.remove("_0");
            for (Map.Entry<String, String> e : mp.entrySet()) {
                Integer v = Integer.valueOf(e.getValue());
                freqDist.inc(e.getKey(), v);
            }
        }

        StringBuilder sb = new StringBuilder(DateUtils.getCurrentDateStr()).append(" daily redis stat report =========\n");
        sb.append(BanyanTypeUtil.prettySortStringifyMap(freqDist));
        String title = "[BANYAN] Redis 环节监控日统计";
        Emailer.sendMonitorEmail(title, sb.toString());
    }

    private boolean isLatestOfDay() {
        Calendar calendar = Calendar.getInstance();
        int h = calendar.get(Calendar.HOUR_OF_DAY);
        int m = calendar.get(Calendar.MINUTE);
        if (h == 0 && m == 0 || h == 23 && m == 59 || testMode) {
            return true;
        } else
            return false;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        RedisMetricReporter redisMetricReporter = new RedisMetricReporter();
        redisMetricReporter.init();
        redisMetricReporter.start();
//        RedisMetricReporter.testMode = true;
//        redisMetricReporter.execute();
        System.out.println("[PROGRAM] Program exited.");
    }
}

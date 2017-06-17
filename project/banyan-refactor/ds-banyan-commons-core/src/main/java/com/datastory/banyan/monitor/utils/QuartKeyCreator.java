package com.datastory.banyan.monitor.utils;

import com.datastory.banyan.redis.RedisConsts;
import com.datastory.banyan.redis.RedisDao;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/**
 * com.datatub.rhino.monitor.utils.QuartKeyCreator
 *
 * @author lhfcws
 * @since 2016/11/4
 */
public class QuartKeyCreator implements QuartzExecutor {
    private String cron = "0 30 * * * ?";
    private RedisDao redisDao;
    private JobDetail jobDetail;

    public QuartKeyCreator(RedisDao redisDao) {
        this.redisDao = redisDao;
    }

    public void start() {
        try {
            jobDetail = QuartzJobUtils.createQuartzJob(cron, "QuartKeyCreator", this);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        if (jobDetail != null) {
            try {
                QuartzJobUtils.removeQuartzJob(jobDetail);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void execute() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, 40);
        Date date = calendar.getTime();
        String dateKey = key(date);
        HashMap<String, String> data = new HashMap<>();
        data.put("_0", "_0");
        try {
            redisDao.hmset(dateKey, data);
            redisDao.expire(dateKey, RedisConsts.DFT_EXPIRE_SEC);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String key() {
        return key(RedisConsts.METRICS_MON_KEY);
    }

    public static String key(Date date) {
        return key(RedisConsts.METRICS_MON_KEY, date);
    }

    public static String key(String prefix) {
        return prefix + DateUtils.getCurrentHourStr();
    }

    public static String key(String prefix, Date date) {
        return prefix + DateUtils.getHourStr(date);
    }
}

package com.datastory.banyan.weibo.util;

import com.datastory.banyan.utils.DateUtils;

import java.io.Serializable;
import java.util.Calendar;

/**
 * com.datastory.banyan.weibo.util.RecentDaysStrategy
 *
 * @author lhfcws
 * @since 16/11/30
 */

public class RecentDaysStrategy implements Serializable {
    private int day = 3;

    public RecentDaysStrategy() {
    }

    public RecentDaysStrategy(int day) {
        this.day = day;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getEarliestUpdateDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -day);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return DateUtils.getTimeStr(calendar.getTime());
    }
}

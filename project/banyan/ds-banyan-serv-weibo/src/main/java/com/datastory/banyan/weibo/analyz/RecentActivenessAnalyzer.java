package com.datastory.banyan.weibo.analyz;

import com.google.common.reflect.TypeToken;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

/**
 * com.datastory.banyan.weibo.analyz.RecentActivenessAnalyzer
 *
 * 判断逻辑：按如下顺序 if-else
 * 最近60d 平均每天发一条以上 => DAY_ACTIVE
 * 最近60d 平均每周发一条以上 => WEEK_ACTIVE
 * 最近60d 平均每月发一条以上 => MONTH_ACTIVE
 * else => NOT_ACTIVE
 *
 * @author lhfcws
 * @since 2016/12/28
 */
public class RecentActivenessAnalyzer {
    private static final Logger LOG = Logger.getLogger(RecentActivenessAnalyzer.class);
    private static final long ONE_DAY = 86400000L;
    private static final long ONE_MONTH = ONE_DAY * 30L;
    private static final int PERIOD = 60;
    private static final DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    public static ACTIVENESS extractWithSortedTweetTime(List<Long> tweetTimes) {
        ACTIVENESS activeness = ACTIVENESS.NOT_ACTIVE;
        if (CollectionUtil.isNotEmpty(tweetTimes)) {
            long now = System.currentTimeMillis();
            int size = tweetTimes.size();
            Long lastTweetTime = tweetTimes.get(size - 1);
            int firstIndex = 0;
            if (size > PERIOD) {
                firstIndex = size - PERIOD;
            }

            int i = firstIndex;
            for (; i < size; i++) {
                if (now - tweetTimes.get(i) <= ONE_MONTH * 2) {
                    break;
                }
            }
            firstIndex = i;
            if (firstIndex >= size)
                return ACTIVENESS.NOT_ACTIVE;

            Long firstTweetTime = tweetTimes.get(firstIndex);
            long sofar = now - lastTweetTime;

            try {
                if (sofar > ONE_MONTH * 2) {
                    return ACTIVENESS.NOT_ACTIVE;
                } else {
                    float recentCount = size - firstIndex + 1;
//                    float recentDays = (float) Math.max(1, (lastTweetTime - firstTweetTime) / ONE_DAY);
//                    float recentDays = size;
                    float recentRate = PERIOD / recentCount;
//                    float avgRate = (totalRate + recentRate) / 2.0F;
                    float avgRate = recentRate;
                    if (avgRate > 30)
                        return ACTIVENESS.NOT_ACTIVE;

                    if (avgRate > 7 || (avgRate <= 7 && sofar > 14 * ONE_DAY)) {
                        return ACTIVENESS.MONTH_ACTIVE;
                    }

                    if (avgRate > 1 || (avgRate <= 1 && sofar > 5 * ONE_DAY)) {
                        return ACTIVENESS.WEEK_ACTIVE;
                    }

                    if (avgRate <= 1)
                        return ACTIVENESS.DAY_ACTIVE;
                }
            } catch (Exception ignore) {
            }
        }

        return activeness;
    }

    public enum ACTIVENESS {
        NOT_ACTIVE(0), MONTH_ACTIVE(1), WEEK_ACTIVE(2), DAY_ACTIVE(3);

        private int value;

        ACTIVENESS(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -30);
        List<Long> tweetTimes = new ArrayList<>();
        Random r = new Random();
        for (int i = 0; i < 60; i++) {
            int m = r.nextInt(4);
            for (int j = 0; j < m; j++) {
                int h = r.nextInt(23);
                calendar.set(Calendar.HOUR_OF_DAY, h);
                tweetTimes.add(calendar.getTimeInMillis());
            }
            calendar.add(Calendar.DATE, -1);
        }

        String json = "[1483459200000, 1481126400000, 1480521600000, 1477497600000]";
        tweetTimes = FastJsonSerializer.deserialize(json, new TypeToken<List<Long>>(){}.getType());
        System.out.println(tweetTimes.size());
        Collections.sort(tweetTimes);
        ACTIVENESS activeness = RecentActivenessAnalyzer.extractWithSortedTweetTime(tweetTimes);
        System.out.println(activeness + " " + activeness.getValue());
        System.out.println("[PROGRAM] Program exited.");
    }
}

package com.datastory.banyan.weibo.analyz.component;

import com.datastory.banyan.weibo.analyz.RecentActivenessAnalyzer;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.util.Util;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.StrParams;

import java.util.*;

/**
 * com.datastory.banyan.weibo.analyz.component.RecentActivenessComponentAnalyzer
 * 只统计最近一个指定周期（默认60day）的活跃度
 *
 * @author lhfcws
 * @since 2016/12/28
 */
public class RecentActivenessComponentAnalyzer implements ComponentAnalyzer {
    @Override
    public List<String> getInputFields() {
        return Arrays.asList("publish_date");
    }

    public transient List<Long> tweetTimes = new ArrayList<>();

    @Override
    public StrParams analyz(String uid, Iterable<? extends Object> valueIter) {
        setup();
        if (valueIter != null) {
            for (Object o : valueIter) {
                streamingAnalyz(o);
            }
        }
        return cleanup();
    }

    @Override
    public void setup() {
        tweetTimes = new ArrayList<>();
    }

    @Override
    public void streamingAnalyz(Object o) {
        try {
            StrParams p = Util.transform(o);
            String pdate = p.get("publish_date");
            if (pdate == null)
                return;
            Date d = DateUtils.parse(pdate, DateUtils.DFT_TIMEFORMAT);
            tweetTimes.add(d.getTime());
        } catch (Exception ignore) {
        }
    }

    @Override
    public StrParams cleanup() {
        if (!CollectionUtil.isEmpty(tweetTimes))
            Collections.sort(tweetTimes);

        RecentActivenessAnalyzer.ACTIVENESS activeness = RecentActivenessAnalyzer.extractWithSortedTweetTime(tweetTimes);
        tweetTimes.clear();
        return new StrParams("activeness", "" + activeness.getValue());
    }
}

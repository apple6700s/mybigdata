package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.monitor.stat.MonStat;
import com.datastory.banyan.monitor.utils.MonUtil;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import com.datastory.banyan.utils.Args;

import static com.datastory.banyan.monitor.MonConsts.M_PH_OUT;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixMonitor2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class PhoenixMonitor2Hook extends RequestHook {
    final String redisKey;

    public PhoenixMonitor2Hook(String table) {
        redisKey = MonConsts.keys(M_PH_OUT, table);
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        RequestList<Args> reqList1 = reqList;
        int[] res = (int[]) ack.getAcks();

        MonStat monStat = new MonStat();
        for (int i = 0; i < reqList.size(); i++) {
            int r = res[i];
            Request<Args> req = reqList1.get(i);

            if (!AckUtil.isJDBCSuccess(r))
                if (req.canRetry()) {
                    monStat.incRetry(1);
                } else
                    monStat.incFail(1);
            else
                monStat.incSuccess(1);
        }

        monStat.incTotal(monStat.getSuccess() + monStat.getFail());
        MonUtil.store(redisKey, monStat);
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<Args> reqList1 = reqList;

        MonStat monStat = new MonStat();
        for (int i = 0; i < reqList.size(); i++) {
            Request<Args> req = reqList1.get(i);

            if (req.canRetry()) {
                monStat.incRetry(1);
            } else
                monStat.incFail(1);
        }

        monStat.incTotal(monStat.getSuccess() + monStat.getFail());
        MonUtil.store(redisKey, monStat);
    }
}

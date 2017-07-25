package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.monitor.stat.MonStat;
import com.datastory.banyan.monitor.utils.MonUtil;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import com.datastory.banyan.req.impl.PutAck;
import org.apache.hadoop.hbase.client.Put;

import static com.datastory.banyan.monitor.MonConsts.M_HB_OUT;

/**
 * com.datastory.banyan.hbase.hooks.HBaseMonitor2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class HBaseMonitor2Hook extends RequestHook {
    final String redisKey;

    public HBaseMonitor2Hook(String table) {
        redisKey = MonConsts.keys(M_HB_OUT, table);
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        PutAck putAck = (PutAck) ack;
        int res = putAck.getAcks();
        RequestList<Put> reqList1 = (RequestList<Put>)reqList;
        Request<Put> req = reqList1.get(0);

        MonStat monStat = new MonStat();
        if (!AckUtil.isHBaseSuccess(res)) {
            if (req.canRetry())
                monStat.incRetry(reqList1.size());
            else
                monStat.incFail(reqList1.size());
        } else
            monStat.incSuccess(reqList1.size());
        monStat.incTotal(monStat.getFail() + monStat.getSuccess());
        MonUtil.store(redisKey, monStat);
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<Put> reqList1 = (RequestList<Put>)reqList;
        Request<Put> req = reqList1.get(0);

        MonStat monStat = new MonStat();
        if (req.canRetry())
            monStat.incRetry(reqList1.size());
        else
            monStat.incFail(reqList1.size());

        monStat.incTotal(monStat.getFail());
        MonUtil.store(redisKey, monStat);
    }
}

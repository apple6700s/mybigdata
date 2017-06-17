package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import com.datastory.banyan.utils.Args;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixRetry2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class PhoenixRetry2Hook extends RequestHook {
    protected PhoenixWriter writer;

    public PhoenixRetry2Hook(PhoenixWriter writer) {
        this.writer = writer;
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        RequestList<Args> reqList1 = reqList;
        int[] res = (int[]) ack.getAcks();

        for (int i = 0; i < reqList.size(); i++) {
            int r = res[i];
            Request<Args> req = reqList1.get(i);

            if (!AckUtil.isJDBCSuccess(r))
                if (req.canRetry()) {
                    retry(req);
                }
        }
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<Args> reqList1 = reqList;

        for (int i = 0; i < reqList.size(); i++) {
            Request<Args> req = reqList1.get(i);

            if (req.canRetry()) {
                retry(req);
            }
        }
    }

    protected void retry(Request<Args> req) {
        writer.batchWrite(req);
    }
}

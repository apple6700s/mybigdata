package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.MainRequestHook;
import com.datastory.banyan.utils.Args;

import java.util.List;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixMain2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class PhoenixMain2Hook extends MainRequestHook {

    public PhoenixMain2Hook(List<DataSinkWriteHook> hooks) {
        super(hooks);
    }

    public PhoenixMain2Hook(DataSinkWriteHook... hooks) {
        super(hooks);
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        RequestList<Args> reqList1 = reqList;
        int[] res = (int[]) ack.getAcks();

        for (int i = 0; i < reqList.size(); i++) {
            int r = res[i];
            Request<Args> req = reqList1.get(i);

            if (!AckUtil.isJDBCSuccess(r))
                req.addRetry();
            else
                req.disableRetry();
        }

        super.afterWriteAck(reqList, ack);
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<Args> reqList1 = reqList;

        for (int i = 0; i < reqList.size(); i++) {
            Request<Args> req = reqList1.get(i);

            req.disableRetry();
        }
        super.afterWriteThrowable(reqList, throwable);
    }
}

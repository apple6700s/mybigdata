package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.MainRequestHook;
import com.datastory.banyan.req.impl.PutAck;
import com.yeezhao.commons.util.Function;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * com.datastory.banyan.hbase.hooks.HBaseMain2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class HBaseMain2Hook extends MainRequestHook {
    public HBaseMain2Hook(List<DataSinkWriteHook> hooks) {
        super(hooks);
    }

    public HBaseMain2Hook(DataSinkWriteHook... hooks) {
        super(hooks);
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        PutAck putAck = (PutAck) ack;
        int res = putAck.getAcks();
        RequestList<Put> resList1 = (RequestList<Put>)reqList;

        if (!AckUtil.isHBaseSuccess(res)) {
            resList1.map(new Function<Request<Put>, Void>() {
                @Override
                public Void apply(Request<Put> putRequest) {
                    putRequest.addRetry();
                    return null;
                }
            });
        }

        super.afterWriteAck(reqList, ack);
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<Put> resList1 = (RequestList<Put>)reqList;
        resList1.map(new Function<Request<Put>, Void>() {
            @Override
            public Void apply(Request<Put> putRequest) {
                putRequest.addRetry();
                return null;
            }
        });
        super.afterWriteThrowable(reqList, throwable);
    }
}

package com.datastory.banyan.req.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.utils.PatternMatch;

/**
 * com.datastory.banyan.req.hooks.RequestHook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public abstract class RequestHook implements DataSinkWriteHook {
    @Override
    public void beforeWrite(Object writeRequest) {

    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {
        PatternMatch.create(RequestList.class, Throwable.class, new PatternMatch.MatchFunc2<RequestList, Throwable>() {
            @Override
            public void _apply(RequestList in1, Throwable in2) {
                afterWriteThrowable(in1, in2);
            }
        }).apply(writeRequest, writeResponse);

        if (writeResponse != null) {
            PatternMatch.create(RequestList.class, Ack.class, new PatternMatch.MatchFunc2<RequestList, Ack>() {
                @Override
                public void _apply(RequestList in1, Ack in2) {
                    afterWriteAck(in1, in2);
                }
            }).apply(writeRequest, writeResponse);
        }
    }

    public abstract void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException;

    public abstract void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException;
}

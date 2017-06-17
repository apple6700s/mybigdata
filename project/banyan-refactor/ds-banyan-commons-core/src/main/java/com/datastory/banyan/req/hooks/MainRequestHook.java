package com.datastory.banyan.req.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.RequestList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * com.datastory.banyan.req.hooks.MainRequestHook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class MainRequestHook extends RequestHook {
    protected List<DataSinkWriteHook> hooks = new ArrayList<>();

    public MainRequestHook(List<DataSinkWriteHook> hooks) {
        this.hooks = hooks;
    }

    public MainRequestHook(DataSinkWriteHook... hooks) {
        this.hooks.addAll(Arrays.asList(hooks));
    }

    @Override
    public void beforeWrite(Object writeRequest) {
        // invoke hooks
        for (DataSinkWriteHook hook : hooks) {
            hook.beforeWrite(writeRequest);
        }
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        if (!reqList.isEmpty())
            // invoke hooks
            for (DataSinkWriteHook hook : hooks) {
                hook.afterWrite(reqList, ack);
            }
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        if (!reqList.isEmpty())
            // invoke hooks
            for (DataSinkWriteHook hook : hooks) {
                hook.afterWrite(reqList, throwable);
            }
    }
}

package com.datastory.banyan.req;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.DsRetryable;

/**
 * com.datastory.banyan.req.Request
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class Request<T> implements DsRetryable {
    private int retryCnt = 0;

    private T requestObj;

    public Request(T requestObj) {
        this.requestObj = requestObj;
    }

    @Override
    public void addRetry() {
        retryCnt++;
    }

    @Override
    public void disableRetry() {
        retryCnt += RhinoETLConsts.MAX_IMPORT_RETRY + 1;
    }

    @Override
    public boolean isRetried() {
        return retryCnt > 0;
    }

    @Override
    public int getRetryCnt() {
        return retryCnt;
    }

    @Override
    public boolean canRetry() {
        return retryCnt <= RhinoETLConsts.MAX_IMPORT_RETRY;
    }

    public T getRequestObj() {
        return requestObj;
    }

    @Override
    public String toString() {
        return "{" +
                "retryCnt=" + retryCnt +
                ", request=" + requestObj +
                '}';
    }
}

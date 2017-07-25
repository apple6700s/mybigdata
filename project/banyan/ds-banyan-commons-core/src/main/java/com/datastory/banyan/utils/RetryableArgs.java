package com.datastory.banyan.utils;

import com.datastory.banyan.base.RhinoETLConsts;

/**
 * com.datastory.banyan.utils.RetryableArgs
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class RetryableArgs extends Args implements DsRetryable {
    public RetryableArgs(Object... arr) {
        super(arr);
    }

    public RetryableArgs(Args args1) {
        super(args1);
    }

    public RetryableArgs(RetryableArgs args1) {
        super(args1);
        this.retryCnt = args1.retryCnt;
    }

    public RetryableArgs() {
    }

    private transient int retryCnt = 0;

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
}

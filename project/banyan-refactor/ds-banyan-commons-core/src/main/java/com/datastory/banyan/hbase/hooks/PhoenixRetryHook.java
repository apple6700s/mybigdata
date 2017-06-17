package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.utils.Args;
import com.datastory.banyan.utils.PatternMatch;
import com.datastory.banyan.utils.RetryableArgs;

import java.util.List;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixRetryHook
 *
 * @author lhfcws
 * @since 2016/12/29
 */
public class PhoenixRetryHook implements DataSinkWriteHook {
    private PhoenixWriter writer;

    public PhoenixRetryHook(PhoenixWriter writer) {
        this.writer = writer;
    }

    @Override
    public void beforeWrite(Object writeRequest) {

    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {
        PatternMatch.create(Args.ArgsList.class, int[].class, new PatternMatch.MatchFunc2<Args.ArgsList, int[]>() {
            @Override
            public void _apply(Args.ArgsList in1, int[] res) {
                int i = 0;
                if (res != null)
                    for (int status : res) {
                        RetryableArgs retryableArgs = (RetryableArgs) in1.get(i);
                        if (!Status.isJDBCSuccess(status) && retryableArgs.canRetry()) {
                            writer.batchWrite(retryableArgs);
                        }
                        i++;
                    }
            }
        }).apply(writeRequest, writeResponse);
    }
}

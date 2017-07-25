package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.utils.Args;
import com.datastory.banyan.utils.PatternMatch;
import com.datastory.banyan.utils.RetryableArgs;

import java.util.ArrayList;
import java.util.List;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixMainHook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
@Deprecated
public class PhoenixMainHook implements DataSinkWriteHook {
    private List<DataSinkWriteHook> hooks = new ArrayList<>();

    public PhoenixMainHook(PhoenixWriter writer) {
        hooks.add(new PhoenixMonitorHook(writer.getTable()));
        hooks.add(new PhoenixRetryHook(writer));
        hooks.add(new PhoenixRetryLogHook(writer.getTable()));
    }

    @Override
    public void beforeWrite(Object writeRequest) {
        for (DataSinkWriteHook hook : hooks) {
            hook.beforeWrite(writeRequest);
        }
    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {

        PatternMatch.create(Args.ArgsList.class, int[].class, new PatternMatch.MatchFunc2<Args.ArgsList, int[]>() {
            @Override
            public void _apply(Args.ArgsList in1, int[] in2) {
                // set retry mark
                int i = -1;
                for (Args args : in1) {
                    i++;
                    if (!Status.isJDBCSuccess(in2[i])) {
                        RetryableArgs retryableArgs = (RetryableArgs) args;
                        retryableArgs.addRetry();
                    }
                }
            }
        }).apply(writeRequest, writeResponse);

        PatternMatch.create(Args.ArgsList.class, Throwable.class, new PatternMatch.MatchFunc2<Args.ArgsList, Throwable>() {
            @Override
            public void _apply(Args.ArgsList in1, Throwable in2) {
                // set retry mark
                for (Args args : in1) {
                    RetryableArgs retryableArgs = (RetryableArgs) args;
                    retryableArgs.addRetry();
                }
            }
        }).apply(writeRequest, writeResponse);

        // invoke hooks
        for (DataSinkWriteHook hook : hooks) {
            hook.afterWrite(writeRequest, writeResponse);
        }
    }
}

package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.retry.record.HDFSLog;
import com.datastory.banyan.utils.Args;
import com.datastory.banyan.utils.PatternMatch;
import com.datastory.banyan.utils.RetryableArgs;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixRetryLogHook
 *
 * @author lhfcws
 * @since 16/12/7
 */
@Deprecated
public class PhoenixRetryLogHook implements DataSinkWriteHook {
    protected static Logger LOG = Logger.getLogger(PhoenixRetryLogHook.class);
    protected String table;

    public PhoenixRetryLogHook(String table) {
        this.table = table;
    }

    @Override
    public void beforeWrite(Object writeRequest) {

    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {
        PatternMatch.create(Args.ArgsList.class, int[].class, new PatternMatch.MatchFunc2<Args.ArgsList, int[]>() {
            @Override
            public void _apply(Args.ArgsList clist, int[] res) {
                HDFSLog log = null;
                if (res == null || res.length == 0) {
                    for (Args argsObj : clist) {
                        RetryableArgs retryableArgs = (RetryableArgs) argsObj;
                        if (!retryableArgs.canRetry()) {
                            if (log == null)
                                log = HDFSLog.get("hbase." + table);
                            log.log(GsonSerializer.serialize(retryableArgs));
                        }
                    }
                } else {
                    int i = 0;
                    for (Args argsObj : clist) {
                        RetryableArgs retryableArgs = (RetryableArgs) argsObj;
                        if (!Status.isJDBCSuccess(res[i]) && !retryableArgs.canRetry()) {
                            if (log == null)
                                log = HDFSLog.get("hbase." + table);
                            log.log(GsonSerializer.serialize(retryableArgs));
                        }
                        i++;
                    }
                }

                if (log != null) {
                    try {
                        log.flush();
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                }
            }
        }).apply(writeRequest, writeResponse);
    }
}

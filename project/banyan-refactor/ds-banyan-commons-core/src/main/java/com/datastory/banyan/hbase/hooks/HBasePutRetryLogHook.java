package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.doc.PutRDocMapper;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.monitor.Status;
import com.datastory.banyan.retry.record.HDFSLog;
import com.datastory.banyan.utils.PatternMatch;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import java.io.IOException;
import java.util.List;

/**
 * com.datastory.banyan.hbase.hooks.HBasePutRetryLogHook
 *
 * @author lhfcws
 * @since 16/12/8
 */

public class HBasePutRetryLogHook implements DataSinkWriteHook {
    private static Logger LOG = Logger.getLogger(HBasePutRetryLogHook.class);
    private String key;

    public HBasePutRetryLogHook(String key) {
        this.key = key;
    }

    @Override
    public void beforeWrite(Object writeRequest) {

    }

    @Override
    public void afterWrite(Object writeRequest, Object writeResponse) {
        PatternMatch.create(List.class, Boolean.class, new PatternMatch.MatchFunc2<List, Boolean>() {
            @Override
            public void _apply(List clist, Boolean res) {
                if (clist.size() > 0) {
                    HDFSLog hdfsLog = null;
                    for (Object o : clist) {
                        Params p = (Params)o;
                        if (hdfsLog == null)
                            hdfsLog = HDFSLog.get(key);
                        hdfsLog.log(FastJsonSerializer.serialize(p));
                    }
                    try {
                        if (hdfsLog != null)
                            hdfsLog.flush();
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                }
            }
        }).apply(writeRequest, writeResponse);

        PatternMatch.create(List.class, Integer.class, new PatternMatch.MatchFunc2<List, Integer>() {
            @Override
            public void _apply(List clist, Integer res) {
                if (clist.size() > 0 && !Status.isHBaseSuccess(res)) {
                    HDFSLog hdfsLog = null;
                    for (Object o : clist) {
                        Put put = (Put) o;
                        Params p = new PutRDocMapper(put).map();
                        if (hdfsLog == null)
                            hdfsLog = HDFSLog.get(key);
                        hdfsLog.log(FastJsonSerializer.serialize(p));
                    }
                    try {
                        if (hdfsLog != null)
                            hdfsLog.flush();
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                }
            }
        }).apply(writeRequest, writeResponse);

        PatternMatch.create(List.class, int[].class, new PatternMatch.MatchFunc2<List, int[]>() {
            @Override
            public void _apply(List clist, int[] reses) {
                if (clist == null || reses == null || clist.size() == 0 || reses.length == 0) return;
                int res = reses[0];
                if (!Status.isHBaseSuccess(res)) {
                    HDFSLog hdfsLog = null;
                    for (Object o : clist) {
                        Put put = (Put) o;
                        Params p = new PutRDocMapper(put).map();
                        if (hdfsLog == null)
                            hdfsLog = HDFSLog.get(key);
                        hdfsLog.log(FastJsonSerializer.serialize(p));
                    }
                    try {
                        if (hdfsLog != null)
                            hdfsLog.flush();
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                }
            }
        }).apply(writeRequest, writeResponse);

    }
}

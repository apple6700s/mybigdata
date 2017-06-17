package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.monitor.stat.MonStat;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import com.datastory.banyan.retry.record.HDFSLog;
import com.datastory.banyan.utils.Args;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.GsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * com.datastory.banyan.hbase.hooks.PhoenixRetryLog2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class PhoenixRetryLog2Hook extends RequestHook {
    protected String table;
    protected List<String> fields;

    public PhoenixRetryLog2Hook(PhoenixWriter writer) {
        this.table = writer.getTable();
        this.fields = new ArrayList<>(writer.getFields());
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        HDFSLog hdfsLog = null;
        RequestList<Args> reqList1 = reqList;
        int[] res = (int[]) ack.getAcks();

        for (int i = 0; i < reqList.size(); i++) {
            int r = res[i];
            Request<Args> req = reqList1.get(i);

            if (!AckUtil.isJDBCSuccess(r))
                if (!req.canRetry()) {
                    Params p = args2Params(req.getRequestObj());
                    String json = GsonSerializer.serialize(p);

                    if (hdfsLog == null) {
                        hdfsLog = HDFSLog.get("hbase." + table);
                    }
                    hdfsLog.log(json);
                }
        }

        if (hdfsLog != null)
            try {
                hdfsLog.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        HDFSLog hdfsLog = null;
        RequestList<Args> reqList1 = reqList;

        for (int i = 0; i < reqList.size(); i++) {
            Request<Args> req = reqList1.get(i);
            if (!req.canRetry()) {
                Params p = args2Params(req.getRequestObj());
                String json = GsonSerializer.serialize(p);

                if (hdfsLog == null) {
                    hdfsLog = HDFSLog.get("hbase." + table);
                }
                hdfsLog.log(json);
            }
        }

        if (hdfsLog != null)
            try {
                hdfsLog.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public Params args2Params(Args args) {
        Params p = new Params();
        for (int i = 0; i < fields.size(); i++) {
            p.put(fields.get(i), args.get(i));
        }
        return p;
    }
}

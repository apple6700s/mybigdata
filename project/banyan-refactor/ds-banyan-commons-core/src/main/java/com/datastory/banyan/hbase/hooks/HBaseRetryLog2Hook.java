package com.datastory.banyan.hbase.hooks;

import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import com.datastory.banyan.req.impl.PutAck;
import com.datastory.banyan.retry.record.HDFSLog;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;


/**
 * com.datastory.banyan.hbase.hooks.HBaseMonitor2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class HBaseRetryLog2Hook extends RequestHook {
    protected String table;

    public HBaseRetryLog2Hook(String table) {
        this.table = table;
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        PutAck putAck = (PutAck) ack;
        int res = putAck.getAcks();
        RequestList<Put> reqList1 = (RequestList<Put>) reqList;
        Request<Put> req = reqList1.get(0);

        HDFSLog hdfsLog = null;
        if (!AckUtil.isHBaseSuccess(res)) {
            if (!req.canRetry()) {
                hdfsLog = HDFSLog.get("hbase." + table);
                for (Request<Put> req1 : reqList1) {
                    retryLog(hdfsLog, req1);
                }
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
        RequestList<Put> reqList1 = (RequestList<Put>) reqList;
        Request<Put> req = reqList1.get(0);
        HDFSLog hdfsLog = null;
        if (!req.canRetry()) {
            hdfsLog = HDFSLog.get("hbase." + table);
            for (Request<Put> req1 : reqList1) {
                retryLog(hdfsLog, req1);
            }
        }

        if (hdfsLog != null)
            try {
                hdfsLog.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public void retryLog(HDFSLog hdfsLog, Request<Put> putRequest) {
        Put put = putRequest.getRequestObj();
        Params p = put2Params(put);
        if (p != null) {
            String json = GsonSerializer.serialize(p);
            hdfsLog.log(json);
        }
    }

    public static Params put2Params(Put put) {
        NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
        List<Cell> cells = familyMap.get("r".getBytes());
        if (BanyanTypeUtil.valid(cells)) {
            Params p = new Params();
            p.put("pk", new String(put.getRow()));
            for (Cell cell : cells) {
                byte[] qualifier = cell.getQualifier();
                byte[] value = cell.getValue();
                if (qualifier == null || value == null)
                    continue;

                p.put(Bytes.toString(qualifier), Bytes.toString(value));
            }
            if (p.size() == 1)
                return null;
            return p;
        } else
            return null;
    }
}

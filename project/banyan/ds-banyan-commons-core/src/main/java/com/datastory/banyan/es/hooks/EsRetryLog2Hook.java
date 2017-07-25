package com.datastory.banyan.es.hooks;

import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import com.datastory.banyan.retry.record.HDFSLog;
import com.yeezhao.commons.util.StringUtil;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * com.datastory.banyan.es.hooks.EsRetry2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class EsRetryLog2Hook extends RequestHook {
    static Class<IndexRequest> klass = IndexRequest.class;
    static Field sourceField = null;

    static {
        try {
            sourceField = klass.getDeclaredField("source");
            sourceField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            System.err.println("ES SDK 版本不兼容，请检查 class IndexRequest 有无 source 字段。");
            e.printStackTrace();
        }
    }

    String bulkName;

    public EsRetryLog2Hook(String bulkName) {
        this.bulkName = bulkName;
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        HDFSLog hdfsLog = null;
        BulkResponse bulkResponse = (BulkResponse) ack.getAcks();
        BulkItemResponse[] res = bulkResponse.getItems();
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;

        for (int i = 0; i < reqList.size(); i++) {
            Request<ActionRequest> req = reqList1.get(i);
            BulkItemResponse r = res[i];
            if (!AckUtil.isBulkResponseSuccess(r)) {
                if (!req.canRetry() && (req.getRequestObj() instanceof IndexRequest)) {
                    try {
                        IndexRequest indexRequest = (IndexRequest) req.getRequestObj();
                        BytesReference bytesReference = (BytesReference) sourceField.get(indexRequest);

                        String source = bytesReference.toUtf8();
                        if (source != null)
                            source = source.trim();
                        if (StringUtil.isNullOrEmpty(source))
                            continue;

                        if (hdfsLog == null) {
                            hdfsLog = HDFSLog.get("es." + bulkName);
                        }
                        hdfsLog.log(source);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
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
        HDFSLog hdfsLog = null;
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;
        for (int i = 0; i < reqList.size(); i++) {
            Request<ActionRequest> req = reqList1.get(i);
            if (!req.canRetry() && (req.getRequestObj() instanceof IndexRequest)) {
                try {
                    IndexRequest indexRequest = (IndexRequest) req.getRequestObj();
                    BytesReference bytesReference = (BytesReference) sourceField.get(indexRequest);

                    String source = bytesReference.toUtf8();
                    if (source != null)
                        source = source.trim();
                    if (StringUtil.isNullOrEmpty(source))
                        continue;

                    if (hdfsLog == null) {
                        hdfsLog = HDFSLog.get("es." + bulkName);
                    }
                    hdfsLog.log(source);
                } catch (Exception e) {
                    e.printStackTrace();
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
}

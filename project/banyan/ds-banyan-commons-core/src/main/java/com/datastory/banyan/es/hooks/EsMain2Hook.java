package com.datastory.banyan.es.hooks;

import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.MainRequestHook;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

import java.util.List;


/**
 * com.datastory.banyan.es.hooks.EsMain2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class EsMain2Hook extends MainRequestHook {
    private static Logger LOG = Logger.getLogger(EsMain2Hook.class);

    public EsMain2Hook(List<DataSinkWriteHook> hooks) {
        super(hooks);
    }

    public EsMain2Hook(DataSinkWriteHook... hooks) {
        super(hooks);
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        BulkResponse bulkResponse = (BulkResponse) ack.getAcks();
        BulkItemResponse[] res = bulkResponse.getItems();
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;

        for (int i = 0; i < reqList.size(); i++) {
            Request<ActionRequest> req = reqList1.get(i);
            BulkItemResponse r = res[i];
            if (!AckUtil.isBulkResponseSuccess(r)) {
                if (AckUtil.isDocumentExist(r)) {
                    IndexRequest indexRequest = ((IndexRequest)req.getRequestObj());
                    LOG.error("[DocumentExistException] " + indexRequest.index() + ", " + indexRequest.type() + ", " + indexRequest.id());
                }

                if (AckUtil.canBulkRetry(r)) {
                    req.addRetry();
                } else
                    req.disableRetry();
            } else
                req.disableRetry();
        }

        super.afterWriteAck(reqList, ack);
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;
        if (AckUtil.canBulkRetry(throwable)) {
            for (Request<ActionRequest> req : reqList1) {
                req.addRetry();
            }
        } else {
            for (Request<ActionRequest> req : reqList1) {
                req.disableRetry();
            }
        }

        super.afterWriteThrowable(reqList, throwable);
    }
}

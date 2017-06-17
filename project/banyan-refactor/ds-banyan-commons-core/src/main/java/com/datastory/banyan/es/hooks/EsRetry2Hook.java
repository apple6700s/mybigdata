package com.datastory.banyan.es.hooks;

import com.datastory.banyan.es.BanyanEsBulkClient;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import org.elasticsearch.action.ActionRequest;

/**
 * com.datastory.banyan.es.hooks.EsRetry2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class EsRetry2Hook extends RequestHook {
    BanyanEsBulkClient client;

    public EsRetry2Hook(BanyanEsBulkClient client) {
        this.client = client;
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;

        for (int i = 0; i < reqList.size(); i++) {
            Request<ActionRequest> req = reqList1.get(i);
            if (req.canRetry()) {
                client.add(req);
            }
        }
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;

        for (int i = 0; i < reqList.size(); i++) {
            Request<ActionRequest> req = reqList1.get(i);
            if (req.canRetry()) {
                client.add(req);
            }
        }
    }
}

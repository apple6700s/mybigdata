package com.datastory.banyan.es.hooks;

import com.datastory.banyan.monitor.MonConsts;
import com.datastory.banyan.monitor.stat.MonStat;
import com.datastory.banyan.monitor.utils.MonUtil;
import com.datastory.banyan.req.Ack;
import com.datastory.banyan.req.AckUtil;
import com.datastory.banyan.req.Request;
import com.datastory.banyan.req.RequestList;
import com.datastory.banyan.req.hooks.RequestHook;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

import static com.datastory.banyan.monitor.MonConsts.M_ES_OUT;

/**
 * com.datastory.banyan.es.hooks.EsMonitor2Hook
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class EsMonitor2Hook extends RequestHook {
    protected static Logger LOG = Logger.getLogger(EsMonitor2Hook.class);

    final String redisKey;

    public EsMonitor2Hook(String bulkName) {
        redisKey = MonConsts.keys(M_ES_OUT, bulkName);
    }

    @Override
    public void afterWriteAck(RequestList reqList, Ack ack) throws ClassCastException {
        BulkResponse bulkResponse = (BulkResponse) ack.getAcks();
        BulkItemResponse[] res = bulkResponse.getItems();
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;

        MonStat monStat = new MonStat();
        String errorMsg = null;

        for (int i = 0; i < reqList.size(); i++) {
            Request<ActionRequest> req = reqList1.get(i);
            BulkItemResponse r = res[i];
            if (!AckUtil.isBulkResponseSuccess(r)) {
                if (req.canRetry()) {
                    monStat.incRetry(1);
                } else {
                    monStat.incFail(1);
                    errorMsg = r.getFailureMessage();
                }
            } else
                monStat.incSuccess(1);
        }
        monStat.incTotal(monStat.getFail() + monStat.getSuccess());

        MonUtil.store(redisKey, monStat);
        if (errorMsg != null)
            LOG.error("[HOOK] " + errorMsg);
    }

    @Override
    public void afterWriteThrowable(RequestList reqList, Throwable throwable) throws ClassCastException {
        RequestList<ActionRequest> reqList1 = (RequestList<ActionRequest>) reqList;

        MonStat monStat = new MonStat();

        for (int i = 0; i < reqList.size(); i++) {
            Request<ActionRequest> req = reqList1.get(i);
            if (req.canRetry()) {
                monStat.incRetry(1);
            } else
                monStat.incFail(1);
        }
        monStat.incTotal(monStat.getFail());
        MonUtil.store(redisKey, monStat);
    }
}

package com.datastory.banyan.es.override233;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.DsRetryable;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;

/**
 * com.datastory.banyan.es.override.DsIndexRequest
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class DsIndexRequest extends IndexRequest implements DsRetryable {
    private transient int retryCnt = 0;

    public DsIndexRequest() {
    }

    public DsIndexRequest(ActionRequest request) {
        super(request);
    }

    public DsIndexRequest(IndexRequest indexRequest, ActionRequest originalRequest) {
        super(indexRequest, originalRequest);
    }

    public DsIndexRequest(String index) {
        super(index);
    }

    public DsIndexRequest(String index, String type) {
        super(index, type);
    }

    public DsIndexRequest(String index, String type, String id) {
        super(index, type, id);
    }

    @Override
    public void addRetry() {
        retryCnt++;
    }

    @Override
    public void disableRetry() {
        retryCnt += RhinoETLConsts.MAX_IMPORT_RETRY + 1;
    }

    @Override
    public boolean isRetried() {
        return retryCnt > 0;
    }

    @Override
    public int getRetryCnt() {
        return retryCnt;
    }

    @Override
    public boolean canRetry() {
        return retryCnt <= RhinoETLConsts.MAX_IMPORT_RETRY;
    }
}

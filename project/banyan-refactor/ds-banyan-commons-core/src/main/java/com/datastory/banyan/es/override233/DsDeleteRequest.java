package com.datastory.banyan.es.override233;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.DsRetryable;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;

/**
 * com.datastory.banyan.es.override233.DsDeleteRequest
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class DsDeleteRequest extends DeleteRequest implements DsRetryable {
    private transient int retryCnt = 0;

    @Override
    public void addRetry() {
        retryCnt++;
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
    public void disableRetry() {
        retryCnt += RhinoETLConsts.MAX_IMPORT_RETRY + 1;
    }

    @Override
    public boolean canRetry() {
        return retryCnt <= RhinoETLConsts.MAX_IMPORT_RETRY;
    }

    public DsDeleteRequest() {
    }

    public DsDeleteRequest(String index) {
        super(index);
    }

    public DsDeleteRequest(String index, String type, String id) {
        super(index, type, id);
    }

    public DsDeleteRequest(DeleteRequest request) {
        super(request);
    }

    public DsDeleteRequest(DeleteRequest request, ActionRequest originalRequest) {
        super(request, originalRequest);
    }

    public DsDeleteRequest(ActionRequest request) {
        super(request);
    }
}

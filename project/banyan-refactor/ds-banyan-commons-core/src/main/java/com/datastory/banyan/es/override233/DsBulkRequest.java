package com.datastory.banyan.es.override233;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.DsRetryable;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;

/**
 * com.datastory.banyan.es.override.DsBulkRequest
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class DsBulkRequest extends BulkRequest implements DsRetryable {

    private transient int retryCnt = 0;

    public DsBulkRequest() {
    }

    public DsBulkRequest(ActionRequest request) {
        super(request);
    }

    public void addRetry() {
        retryCnt++;
    }

    public boolean isRetried() {
        return retryCnt > 0;
    }

    public int getRetryCnt() {
        return retryCnt;
    }

    @Override
    public boolean canRetry() {
        return retryCnt <= RhinoETLConsts.MAX_IMPORT_RETRY;
    }

    @Override
    public void disableRetry() {
        retryCnt += RhinoETLConsts.MAX_IMPORT_RETRY + 1;
    }
}

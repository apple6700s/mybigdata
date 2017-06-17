package com.datastory.banyan.es.override233;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.DsRetryable;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * com.datastory.banyan.es.override233.DsUpdateRequest
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class DsUpdateRequest extends UpdateRequest implements DsRetryable {
    private transient int retryCnt = 0;

    public DsUpdateRequest() {
    }

    public DsUpdateRequest(String index, String type, String id) {
        super(index, type, id);
    }

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
    public boolean canRetry() {
        return retryCnt <= RhinoETLConsts.MAX_IMPORT_RETRY;
    }

    @Override
    public void disableRetry() {
        retryCnt += RhinoETLConsts.MAX_IMPORT_RETRY + 1;
    }
}

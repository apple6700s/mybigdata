package com.datastory.banyan.es.hooks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;

/**
 * com.datastory.banyan.es.hooks.IndexRetry
 *
 * @author lhfcws
 * @since 16/12/9
 */

public interface IndexRetry {
    public void retry(IndexRequest ir, ActionListener<IndexResponse> listener);

    public boolean canRetry(Throwable e);

    public BackoffPolicy getIndexBackoffPolicy();
}

package com.datastory.banyan.utils;


/**
 * com.datastory.banyan.es.override.DsRetryable
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public interface DsRetryable {
    void addRetry();

    void disableRetry();

    boolean isRetried();

    int getRetryCnt();

    boolean canRetry();
}

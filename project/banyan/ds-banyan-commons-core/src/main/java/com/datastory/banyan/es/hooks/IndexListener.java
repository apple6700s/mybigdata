package com.datastory.banyan.es.hooks;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Iterator;

/**
 * com.datastory.banyan.es.hooks.IndexListener
 *
 * IndexListener for retry
 * 采用指数退避算法重试，最终失败会调钩子
 *
 * @author lhfcws
 * @since 16/12/9
 */
@Deprecated
public class IndexListener implements ActionListener<IndexResponse> {
    protected ESWriteHook esWriteHook;
    protected Client client;
    protected IndexRequest indexRequest;
    protected Iterator<TimeValue> sleepTimes;

    public IndexListener(ESWriteHook esWriteHook, Client client, IndexRequest indexRequest, Iterator<TimeValue> sleepTimes) {
        this.esWriteHook = esWriteHook;
        this.client = client;
        this.indexRequest = indexRequest;
        this.sleepTimes = sleepTimes;
    }

    public IndexRequest getIndexRequest() {
        return indexRequest;
    }

    @Override
    public void onResponse(IndexResponse indexResponse) {
        if (indexResponse.isCreated()) {
            esWriteHook.afterWrite(getIndexRequest(), 1);
        }
    }

    @Override
    public void onFailure(Throwable e) {
        boolean isRetried = false;
        if (esWriteHook.canRetry(e)) {
            if (sleepTimes.hasNext()) {
                TimeValue tv = sleepTimes.next();
                try {
                    Thread.sleep(tv.getMillis());
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                client.index(getIndexRequest(), this);
                isRetried = true;
            }
        }

        if (!isRetried) {
            esWriteHook.afterWrite(getIndexRequest(), e);
        }
    }
}
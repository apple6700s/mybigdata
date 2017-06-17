package com.datastory.banyan.req.impl;

import com.datastory.banyan.req.Ack;
import org.elasticsearch.action.bulk.BulkResponse;

/**
 * com.datastory.banyan.req.impl.BulkResAck
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class BulkResAck implements Ack {
    private BulkResponse bulkResponse;

    public BulkResAck(BulkResponse bulkResponse) {
        this.bulkResponse = bulkResponse;
    }

    @Override
    public BulkResponse getAcks() {
        return bulkResponse;
    }
}

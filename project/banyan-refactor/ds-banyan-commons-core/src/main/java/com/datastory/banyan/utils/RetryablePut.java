package com.datastory.banyan.utils;

import org.apache.hadoop.hbase.client.Put;

import java.nio.ByteBuffer;

/**
 * com.datastory.banyan.utils.RetryablePut
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class RetryablePut extends Put {
    public RetryablePut(byte[] row) {
        super(row);
    }

    public RetryablePut(byte[] row, long ts) {
        super(row, ts);
    }

    public RetryablePut(byte[] rowArray, int rowOffset, int rowLength) {
        super(rowArray, rowOffset, rowLength);
    }

    public RetryablePut(ByteBuffer row, long ts) {
        super(row, ts);
    }

    public RetryablePut(ByteBuffer row) {
        super(row);
    }

    public RetryablePut(byte[] rowArray, int rowOffset, int rowLength, long ts) {
        super(rowArray, rowOffset, rowLength, ts);
    }

    public RetryablePut(Put putToCopy) {
        super(putToCopy);
    }
}

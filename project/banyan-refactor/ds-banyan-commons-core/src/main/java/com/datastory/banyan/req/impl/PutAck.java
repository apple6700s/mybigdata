package com.datastory.banyan.req.impl;

import com.datastory.banyan.req.Ack;

/**
 * com.datastory.banyan.req.impl.PutAck
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class PutAck implements Ack {
    private int ack;

    public PutAck(int ack) {
        this.ack = ack;
    }

    @Override
    public Integer getAcks() {
        return ack;
    }
}

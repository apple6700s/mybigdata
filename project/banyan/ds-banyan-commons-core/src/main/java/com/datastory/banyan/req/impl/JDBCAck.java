package com.datastory.banyan.req.impl;

import com.datastory.banyan.req.Ack;

/**
 * com.datastory.banyan.req.impl.JDBCAck
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class JDBCAck implements Ack {
    private int[] acks;

    public JDBCAck(int[] acks) {
        this.acks = acks;
    }

    @Override
    public int[] getAcks() {
        return acks;
    }
}

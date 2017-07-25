package com.datastory.banyan.kafka;

import com.datastory.banyan.batch.BlockProcessor;

import java.io.Serializable;

/**
 * com.datastory.banyan.kafka.ProcessorRouter
 *
 * @author lhfcws
 * @since 2017/4/12
 */
public interface ProcessorRouter extends Serializable {
    public BlockProcessor route(Object msg);
    public void cleanup();
}

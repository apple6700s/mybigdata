package com.datastory.banyan.kafka;

import java.io.Serializable;
import java.util.HashMap;

/**
 * com.datastory.banyan.kafka.ToESConsumerProcessorMap
 *
 * @author lhfcws
 * @since 16/11/25
 */

public class ToESConsumerProcessorMap extends HashMap<String, ToESConsumerProcessor> implements Serializable {
    public void add(ToESConsumerProcessor processor) {
        this.put(processor.getHBaseReader().getTable(), processor);
    }
}

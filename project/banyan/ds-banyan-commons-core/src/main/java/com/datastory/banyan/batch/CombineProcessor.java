package com.datastory.banyan.batch;

import com.datastory.banyan.utils.CountUpLatch;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.batch.CombineProcessor
 *
 * @author lhfcws
 * @since 2017/7/6
 */
public class CombineProcessor extends CountUpLatchBlockProcessor {
    List<CountUpLatchBlockProcessor> processors = new LinkedList<>();

    public CombineProcessor(CountUpLatch latch, Class<? extends CountUpLatchBlockProcessor>... klasses) throws Exception {
        super(latch);
        add(klasses);
    }

    public CombineProcessor(CountUpLatch latch, CountUpLatchBlockProcessor... processors) {
        super(latch);
        add(processors);
    }

    public CombineProcessor add(CountUpLatchBlockProcessor... processors) {
        this.processors.addAll(Arrays.asList(processors));
        return this;
    }

    public CombineProcessor add(Class<? extends CountUpLatchBlockProcessor>... klasses) throws Exception {
        for (Class<? extends CountUpLatchBlockProcessor> bpClass : klasses) {
            Constructor constructor = bpClass.getConstructor(CountUpLatch.class);
            Object obj = constructor.newInstance(new Object[] {null});
            CountUpLatchBlockProcessor bp = (CountUpLatchBlockProcessor) obj;
            processors.add(bp);
        }
        return this;
    }

    @Override
    public void _process(Object _p) {
        try {
            for (CountUpLatchBlockProcessor processor : processors) {
                processor.process(_p);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        for (CountUpLatchBlockProcessor processor : processors) {
            processor.cleanup();
        }
    }
}

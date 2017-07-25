package com.datastory.banyan.kafka;

import com.datastory.banyan.batch.BlockProcessor;
import com.datastory.banyan.utils.ConstructorFunc;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ThreadPoolWrapper;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * com.datastory.banyan.kafka.LazyInitProcessorRouter
 *
 * @author lhfcws
 * @since 2017/4/26
 */
public abstract class LazyInitProcessorRouter implements ProcessorRouter {
    protected HashMap<String, BlockProcessor> processors = new HashMap<>();
    protected HashMap<String, ConstructorFunc<? extends BlockProcessor>> constructors = new HashMap<>();

    public LazyInitProcessorRouter register(String bpClass, ConstructorFunc<? extends BlockProcessor> constructorFunc) {
        constructors.put(bpClass, constructorFunc);
        return this;
    }

    public LazyInitProcessorRouter registerCountUpLatchBlockProcessor(final CountUpLatch latch, String... bpClasses) {
        for (final String bpClass : bpClasses)
            constructors.put(bpClass, new ConstructorFunc<BlockProcessor>() {
                @Override
                public BlockProcessor construct() {
                    try {
                        Class klass = Class.forName(bpClass);
                        Constructor constructor = klass.getConstructor(CountUpLatch.class);
                        Object obj = constructor.newInstance(latch);
                        BlockProcessor bp = (BlockProcessor) obj;
                        bp.setPool(ThreadPoolWrapper.getInstance(200)).setup();
                        return bp;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return null;
                }
            });
        return this;
    }

    public LazyInitProcessorRouter registerCountUpLatchBlockProcessor(final CountUpLatch latch, Class<? extends BlockProcessor>... bpClasses) {
        for (final Class bpClass : bpClasses)
            constructors.put(bpClass.getCanonicalName(), new ConstructorFunc<BlockProcessor>() {
                @Override
                public BlockProcessor construct() {
                    try {
                        Constructor constructor = bpClass.getConstructor(CountUpLatch.class);
                        Object obj = constructor.newInstance(latch);
                        BlockProcessor bp = (BlockProcessor) obj;
                        bp.setPool(ThreadPoolWrapper.getInstance(200)).setup();
                        return bp;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return null;
                }
            });
        return this;
    }

    public Collection<BlockProcessor> getCurrentProcessors() {
        return processors.values();
    }

    public Set<String> getCurrentProcessorClassNames() {
        return processors.keySet();
    }

    public Set<String> getAllProcessorClassNames() {
        return constructors.keySet();
    }

    public BlockProcessor lazyInit(Class<? extends BlockProcessor> processorClass) {
        return lazyInit(processorClass.getCanonicalName());
    }

    public BlockProcessor lazyInit(String bpClass) {
        if (!processors.containsKey(bpClass)) {
            synchronized (this) {
                if (!processors.containsKey(bpClass)) {
                    if (constructors.containsKey(bpClass)) {
                        BlockProcessor bp = constructors.get(bpClass).construct();
                        if (bp != null) {
                            processors.put(bpClass, bp);
                        }
                        return bp;
                    } else
                        return null;
                }
            }
        }
        return processors.get(bpClass);
    }

    @Override
    public void cleanup() {
        constructors.clear();
        for (BlockProcessor bp : processors.values()) {
            bp.cleanup();
        }
        processors.clear();
    }
}

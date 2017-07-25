package com.datastory.banyan.batch;

import com.datastory.banyan.utils.ThreadPoolWrapper;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * com.datastory.banyan.batch.BlockProcessor
 *
 * Not thread-safe, use ThreadLocal
 * @author lhfcws
 * @since 2016/12/20
 */
public abstract class BlockProcessor implements Serializable {
    protected static Logger LOG = Logger.getLogger(BlockProcessor.class);

    protected int batchSize = 500;
    protected AtomicLong processSize = new AtomicLong(0);
    Semaphore semaphore;
    ThreadPoolWrapper pool;

    public BlockProcessor(int batchSize) {
        this.batchSize = batchSize;
        semaphore = new Semaphore(batchSize);
    }

    public BlockProcessor() {
        semaphore = new Semaphore(batchSize);
    }

    public BlockProcessor setPool(ThreadPoolWrapper pool) {
        this.pool = pool;
        return this;
    }

    public long getProcessedSize() {
        return processSize.get();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public BlockProcessor setup() {
        processSize.set(0);
        return this;
    }

    public void process(Object _p) throws Exception {
        semaphore.acquire();
        try {
            processSize.incrementAndGet();
            _process(_p);
        } finally {
            semaphore.release();
        }
    }

    public void processAsync(final Object _p) throws Exception {
        pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    processSize.incrementAndGet();
                    _process(_p);
                } finally {
                    semaphore.release();
                }
            }
        });
    }

    public abstract void _process(Object _p);

    public abstract void cleanup();
}

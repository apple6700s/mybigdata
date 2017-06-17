package com.datastory.banyan.batch;

import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ThreadPoolWrapper;

/**
 * com.datastory.banyan.batch.CountUpLatchBlockProcessor
 *
 * @author lhfcws
 * @since 2017/4/27
 */
public abstract class CountUpLatchBlockProcessor extends BlockProcessor {
    protected CountUpLatch latch;

    public CountUpLatchBlockProcessor(CountUpLatch latch) {
        super();
        this.latch = latch;
    }

    @Override
    public void process(Object _p) throws Exception {
        semaphore.acquire();
        try {
            processSize.incrementAndGet();
            _process(_p);
        } finally {
            if (latch != null)
                latch.countup();
            semaphore.release();
        }
    }

    @Override
    public void processAsync(final Object _p) throws Exception {
        if (pool == null) {
            synchronized (this) {
                if (pool == null) {
                    pool = ThreadPoolWrapper.getInstance(100);
                }
            }
        }

        pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    processSize.incrementAndGet();
                    _process(_p);
                } finally {
                    if (latch != null)
                        latch.countup();
                    semaphore.release();
                }
            }
        });
    }
}

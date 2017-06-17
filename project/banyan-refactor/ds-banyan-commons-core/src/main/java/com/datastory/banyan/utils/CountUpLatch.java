package com.datastory.banyan.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lhfcws on 16/11/15.
 */
public class CountUpLatch {
    private AtomicInteger counter = new AtomicInteger(0);

    public int countup() {
        return counter.incrementAndGet();
    }

    public void await(int limit) {
        while (true) {
            if (counter.get() >= limit)
                break;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

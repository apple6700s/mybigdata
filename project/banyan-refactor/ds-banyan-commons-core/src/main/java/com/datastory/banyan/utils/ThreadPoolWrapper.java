package com.datastory.banyan.utils;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lhfcws on 16/11/14.
 */
public class ThreadPoolWrapper implements Serializable {
    private static volatile ThreadPoolWrapper _singleton = null;

    public static ThreadPoolWrapper getInstance() {
        if (_singleton == null)
            synchronized (ThreadPoolWrapper.class) {
                if (_singleton == null) {
                    _singleton = new ThreadPoolWrapper(150);
                }
            }
        return _singleton;
    }

    public static ThreadPoolWrapper getInstance(int num) {
        if (_singleton == null)
            synchronized (ThreadPoolWrapper.class) {
                if (_singleton == null) {
                    _singleton = new ThreadPoolWrapper(num);
                }
            }
        return _singleton;
    }

    private ExecutorService executorService;

    protected ThreadPoolWrapper(int num) {
        executorService = Executors.newFixedThreadPool(num);
    }

    public void submit(Runnable runnable) {
        executorService.submit(runnable);
    }

    public void shutdown() {
        executorService.shutdown();
    }
}

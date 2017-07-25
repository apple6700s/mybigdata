package com.datastory.banyan.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * com.datastory.banyan.utils.ShutdownHookManger
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class ShutdownHookManger implements Serializable {
    private static volatile ShutdownHookManger _singleton = null;
    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    public static ShutdownHookManger getInstance() {
        if (_singleton == null)
            synchronized (ShutdownHookManger.class) {
                if (_singleton == null) {
                    _singleton = new ShutdownHookManger();
                }
            }
        return _singleton;
    }

    private ShutdownHookManger() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (!isShutdown.get()) {
                    synchronized (ShutdownHookManger.class) {
                        if (!isShutdown.get()) {
                            isShutdown.set(true);
                            HashMap<String, Runnable> _hooks = new HashMap<String, Runnable>(hooks);
                            final int size = _hooks.size();
                            final CountDownLatch latch = new CountDownLatch(size);
                            for (final Map.Entry<String, Runnable> e : _hooks.entrySet()) {
                                new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            System.out.println("[ShutdownHookManager] ShutdownHook ( " + e.getKey() + " ) start.");
                                            e.getValue().run();
                                            System.out.println("[ShutdownHookManager] ShutdownHook ( " + e.getKey() + " ) done.");
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        } finally {
                                            latch.countDown();
                                            long now = size - latch.getCount();
                                            System.out.println("[ShutdownHookManager] Progress " + now + " / " + size);
                                        }
                                    }
                                }).start();
                            }
                            if (size > 0) {
                                try {
                                    latch.await();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                            HaltThread.start(1000);
                        }
                    }
                }
            }
        }));
    }

    ConcurrentHashMap<String, Runnable> hooks = new ConcurrentHashMap<>();


    public static void addShutdownHook(String name, Runnable runnable) {
        if (runnable == null || name == null)
            return;

        ShutdownHookManger mgr = ShutdownHookManger.getInstance();
        if (mgr.hooks.contains(name))
            return;
        mgr.hooks.put(name, runnable);
    }

    public static void removeShutdownHook(String name) {
        if (name == null)
            return;

        ShutdownHookManger mgr = ShutdownHookManger.getInstance();
        mgr.hooks.remove(name);
    }
}

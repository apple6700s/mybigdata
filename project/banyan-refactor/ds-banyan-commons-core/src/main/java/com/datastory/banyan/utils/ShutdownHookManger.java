package com.datastory.banyan.utils;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * com.datastory.banyan.utils.ShutdownHookManger
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class ShutdownHookManger implements Serializable {
    private static volatile ShutdownHookManger _singleton = null;

    public static ShutdownHookManger getInstance() {
        if (_singleton == null)
            synchronized (ShutdownHookManger.class) {
                if (_singleton == null) {
                    _singleton = new ShutdownHookManger();
                }
            }
        return _singleton;
    }

    ConcurrentHashMap<Class, Runnable> hooks = new ConcurrentHashMap<>();

    public static void addShutdownHook(Class klass, Runnable runnable) {
        ShutdownHookManger mgr = ShutdownHookManger.getInstance();
        if (mgr.hooks.contains(klass))
            return;
        mgr.hooks.putIfAbsent(klass, runnable);
        Runtime.getRuntime().addShutdownHook(new Thread(runnable));
    }
}

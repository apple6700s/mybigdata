package com.datastory.banyan.async;

import org.apache.log4j.Logger;

import java.util.concurrent.Future;

/**
 * Javascript-style async.
 *
 * @author lhfcws
 * @since 16/1/12.
 */
public class Async {
    public static final Logger LOG = Logger.getLogger(Async.class);

    public static Future async(AsyncPool pool, final Function func, final Callback succeedCB, final Callback failCB, final Callback exceptionCB) {
        if (func == null) return null;
        return pool.getPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncRet res = func.call();
                    if (res != null) {
                        if (res.isBadStatus()) {
                            if (failCB != null)
                                failCB.callback(res);
                            else if (succeedCB != null)
                                succeedCB.callback(res);
                        } else if (succeedCB != null) {
                            succeedCB.callback(res);
                        }
                    } else {
                        if (succeedCB != null)
                            succeedCB.callback(null);
                    }
                } catch (Exception e) {
                    if (exceptionCB != null)
                        exceptionCB.callback(e);
                    else {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        });
    }

    public static Future async(AsyncPool pool, Function func, Callback succeedCB, Callback failCB) {
        return async(pool, func, succeedCB, failCB, null);
    }

    public static Future async(AsyncPool pool, Function func, Callback callback) {
        return async(pool, func, callback, null, null);
    }

    public static Future async(AsyncPool pool, Function func) {
        return async(pool, func, null, null, null);
    }

    public static Future async(final Function func, final Callback succeedCB, final Callback failCB, final Callback exceptionCB) {
        if (func == null) return null;
        return AsyncPool.RUN_POOL.getPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    AsyncRet res = func.call();
                    if (res != null) {
                        if (res.isBadStatus()) {
                            if (failCB != null)
                                failCB.callback(res);
                            else if (succeedCB != null)
                                succeedCB.callback(res);
                        } else if (succeedCB != null) {
                            succeedCB.callback(res);
                        }
                    } else {
                        if (succeedCB != null)
                            succeedCB.callback(null);
                    }
                } catch (Exception e) {
                    if (exceptionCB != null)
                        exceptionCB.callback(e);
                    else {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        });
    }

    public static Future async(Function func, Callback succeedCB, Callback failCB) {
        return async(func, succeedCB, failCB, null);
    }

    public static Future async(Function func, Callback callback) {
        return async(func, callback, null, null);
    }

    public static Future async(Function func) {
        return async(func, null, null, null);
    }


    public static Future setTimeout(final Function func, final Long waitTime) {
        return AsyncPool.RUN_POOL.getPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(waitTime);
                    func.call();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        });
    }
}

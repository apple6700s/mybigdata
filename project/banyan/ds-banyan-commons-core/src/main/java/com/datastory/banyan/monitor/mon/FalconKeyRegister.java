package com.datastory.banyan.monitor.mon;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by lhfcws on 16/11/14.
 */
public class FalconKeyRegister implements Serializable {
    private static Set<String> keys = Collections.synchronizedSet(new HashSet<String>());

    public static synchronized void register(String key) {
        if (!keys.contains(key)) {
            keys.add(key);
            FalconMetricMonitor.getInstance().register(key);
        }
    }
}

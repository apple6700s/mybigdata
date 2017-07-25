package com.datastory.banyan.validate.stats;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import org.apache.commons.lang.StringUtils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by abel.chan on 17/7/7.
 */
public class StatsFactory {

    private static final AtomicBoolean state = new AtomicBoolean(true);

    private static Stats INSTANCE;

    public static Stats getStatsInstance() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (INSTANCE == null) {
            synchronized (state) {
                if (INSTANCE == null) {
                    RhinoETLConfig config = RhinoETLConfig.getInstance();
                    String className = config.get(RhinoETLConsts.VALIDATE_STATS_INSTANCE);
                    if (StringUtils.isEmpty(className)) {
                        throw new NullPointerException("valid stats class name is null !!!");
                    }
                    Class classType = Class.forName(className);
                    INSTANCE = (Stats) classType.newInstance();
                    if (INSTANCE == null) {
                        throw new IllegalArgumentException("valid stats instance init failed !!!");
                    }
                }
            }
        }
        return INSTANCE;
    }
}

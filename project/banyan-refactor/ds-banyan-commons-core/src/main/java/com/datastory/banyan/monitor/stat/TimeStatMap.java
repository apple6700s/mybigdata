package com.datastory.banyan.monitor.stat;

import java.util.HashMap;
import java.util.Map;

/**
 * com.datastory.banyan.monitor.stat.TimeStatMap
 *
 * @author lhfcws
 * @since 2017/2/6
 */
public class TimeStatMap extends HashMap<String, TimeStat> {
    public TimeStatMap begin(String key) {
        put(key, new TimeStat());
        return this;
    }

    public TimeStatMap end(String key) {
        if (containsKey(key)) {
            get(key).end();
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, TimeStat> e : this.entrySet()) {
            sb.append(e.getValue().toString(e.getKey())).append(" | ");
        }
        return sb.toString();
    }
}

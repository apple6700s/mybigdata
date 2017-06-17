package com.datastory.banyan.monitor.stat;

/**
 * com.datatub.rhino.monitor.stat.TimeStat
 *
 * @author lhfcws
 * @since 2016/10/31
 */
public class TimeStat {
    final long startTime = System.currentTimeMillis();
    long endTime = -1;

    public TimeStat() {
    }

    public long end() {
        return endTime = System.currentTimeMillis();
    }

    public long diff() {
        if (endTime == -1)
            end();
        return  endTime - startTime;
    }

    public String toString(String prefix) {
        String msg = prefix + " cost time (ms) : " + diff();
        return msg;
    }

    public String print(String prefix) {
        String msg = toString(prefix);
        System.out.println(msg);
        return msg;
    }
}

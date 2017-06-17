package com.datastory.banyan.monitor.stat;

import java.io.Serializable;

/**
 * com.datatub.rhino.monitor.stat.AccuStat
 * Accumulate stat
 * @author lhfcws
 * @since 2016/11/8
 */
public class AccuStat implements Serializable {
    protected volatile long value = 0;
    protected long startTime = 0;

    public void inc() {
        this.value += 1;
    }

    public void inc(long v) {
        this.value += v;
    }

    public void begin() {
        this.startTime = System.currentTimeMillis();
    }

    public void end() {
        this.value += (System.currentTimeMillis() - this.startTime);
    }

    public boolean isEmpty() {
        return value == 0 && startTime == 0;
    }

    public AccuStat tsafeBegin() {
        AccuStat s = new AccuStat();
        s.begin();
        return s;
    }

    public void tsafeEnd(AccuStat s) {
        s.end();
        inc(s);
    }

    public long getValue() {
        return value;
    }

    public double getAvg(int size) {
        if (size == 0) return 0;
        return 1.0 * getValue() / size;
    }

    public synchronized AccuStat inc(AccuStat stat) {
        this.value += stat.getValue();
        return this;
    }

    @Override
    public String toString() {
        return "{" +
                "value=" + value +
                ", startTime=" + startTime +
                '}';
    }
}

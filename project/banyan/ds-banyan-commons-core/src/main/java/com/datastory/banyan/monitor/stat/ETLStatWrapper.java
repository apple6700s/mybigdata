package com.datastory.banyan.monitor.stat;

import java.io.Serializable;

/**
 * com.datastory.banyan.monitor.stat.ETLStatWrapper
 *
 * @author lhfcws
 * @since 16/11/23
 */

public class ETLStatWrapper implements Serializable {
    private AccuStat hbaseStat = new AccuStat();
    private AccuStat esStat = new AccuStat();
    private AccuStat filterStat = new AccuStat();
    private AccuStat analyzStat = new AccuStat();
    private AccuStat kfkStat = new AccuStat();
    private AccuStat errStat = new AccuStat();
    private Object value;

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public AccuStat hbaseStat() {
        return hbaseStat;
    }

    public AccuStat esStat() {
        return esStat;
    }

    public AccuStat filterStat() {
        return filterStat;
    }

    public AccuStat analyzStat() {
        return analyzStat;
    }

    public AccuStat kfkStat() {
        return kfkStat;
    }

    public AccuStat errStat() {
        return errStat;
    }

    @Override
    public String toString() {
        return "{" +
                "hbaseStat=" + hbaseStat +
                ", esStat=" + esStat +
                ", filterStat=" + filterStat +
                ", analyzStat=" + analyzStat +
                ", kfkStat=" + kfkStat +
                ", value=" + value +
                '}';
    }
}

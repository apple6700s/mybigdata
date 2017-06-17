package com.datastory.banyan.monitor.stat;

/**
 * com.datastory.banyan.monitor.stat.MonStat
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class MonStat {
    private long total = 0;
    private long success = 0;
    private long fail = 0;
    private long retry = 0;

    public void incRetry(long v) {
        retry += v;
    }

    public void incTotal(long v) {
        total += v;
    }

    public void incSuccess(long v) {
        success += v;
    }

    public void incFail(long v) {
        fail += v;
    }

    public long getRetry() {
        return retry;
    }

    public long getTotal() {
        return total;
    }

    public long getSuccess() {
        return success;
    }

    public long getFail() {
        return fail;
    }

    @Override
    public String toString() {
        return "{" +
                "total=" + total +
                ", success=" + success +
                ", fail=" + fail +
                ", retry=" + retry +
                '}';
    }
}

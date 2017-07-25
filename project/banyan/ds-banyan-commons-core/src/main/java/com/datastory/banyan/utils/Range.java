package com.datastory.banyan.utils;

import java.io.Serializable;

/**
 * com.datatub.rhino.utils.Range
 *
 * @author lhfcws
 * @since 2016/10/17
 */
public class Range implements Serializable {
    String start;
    String end;

    public Range(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public Range(long start, long end) {
        this.start = "" + start;
        this.end = "" + end;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public void setStart(long start) {
        this.start = start + "";
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public void setEnd(long end) {
        this.end = end + "";
    }

    public long getStartLong() {
        return Long.valueOf(start);
    }

    public long getEndLong() {
        return Long.valueOf(end);
    }

    public int getStartInt() {
        return Integer.parseInt(start);
    }

    public int getEndInt() {
        return Integer.parseInt(end);
    }

    @Override
    public String toString() {
        return "Range(" + start + " - " + end + ')';
    }
}

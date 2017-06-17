package com.datastory.banyan.monitor.checker;

/**
 * com.datatub.rhino.monitor.analyz.PercentChecker
 *
 * @author lhfcws
 * @since 2016/11/8
 */
public class PercentChecker {
    protected double threshold = 0.8;

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public double calcRate(double total, double success) {
        return (total - success) / total;
    }

    public double calcRate(String total, String success) {
        return calcRate(Double.valueOf(total), Double.valueOf(success));
    }

    public boolean check(double total, double success) {
        return (total - success) / total <= threshold;
    }

    public boolean check(String total, String success) {
        return check(Double.valueOf(total), Double.valueOf(success));
    }

}

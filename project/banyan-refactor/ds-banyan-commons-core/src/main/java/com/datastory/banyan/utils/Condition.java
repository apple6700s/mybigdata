package com.datastory.banyan.utils;

import java.util.Timer;
import java.util.TimerTask;

/**
 * com.datastory.banyan.utils.Condition
 * 返回值true的时候表示condition被满足。
 * @author lhfcws
 * @since 2017/6/7
 */
public abstract class Condition {
    private long interval = 100;
    private boolean stopCondition = false;

//    public Condition interval(long i) {
//        this.interval = i;
//        return this;
//    }

    public abstract boolean satisfy();

    public void await() throws InterruptedException {
        if (this.satisfy())
            return;
        await(-1);
    }

    public void await(long timeout) throws InterruptedException {
        if (this.satisfy())
            return;

        if (timeout > 0) {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    stopCondition = true;
                }
            }, timeout);
        }

        while (!stopCondition) {
            if (this.satisfy())
                break;

            Thread.sleep(interval);
        }
    }

    public void forceSignal() {
        stopCondition = true;
    }
}


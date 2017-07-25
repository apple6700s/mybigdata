package com.datastory.banyan.utils;

import com.yeezhao.commons.util.RuntimeUtil;

import java.util.Timer;
import java.util.TimerTask;

/**
 * com.datastory.banyan.utils.HaltThread
 *
 * @author lhfcws
 * @since 2017/7/6
 */
public class HaltThread extends TimerTask {
    @Override
    public void run() {
        System.out.println("[HALT] Halt stuck process " + RuntimeUtil.PID);
        System.out.flush();
        Runtime.getRuntime().halt(1);
    }

    public static void start() {
        new Timer().schedule(new HaltThread(), 30 * 1000);
    }

    public static void start(long delay) {
        new Timer().schedule(new HaltThread(), delay);
    }
}

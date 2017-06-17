package com.datastory.banyan.weibo.analyz;

import java.lang.management.ManagementFactory;

/**
 * com.datatub.rhino.analyz.FansLevelTranslator
 *
 * @author lhfcws
 * @since 2016/10/17
 */
public class FansLevelTranslator {
    private static final int DEFAULT_LEVEL = 0;

    public static int getNewFollowersCountRank(String str) {
        try {
            str = str.trim();
            Integer c = Integer.parseInt(str);
            return getNewFollowersCountRank(c);
        } catch (Exception e) {
            return 0;
        }
    }

    public static int getNewFollowersCountRank(long followersCount) {
        if (followersCount <= 0) {
            return 0;
        }
        else if (followersCount < 50) {
            return 1;
        }
        else if (followersCount < 100) {
            return 2;
        }
        else if (followersCount < 200) {
            return 3;
        }
        else if (followersCount < 500) {
            return 4;
        }
        else if (followersCount < 1000) {
            return 5;
        }
        else if (followersCount < 10000) {
            return 6;
        }
        else if (followersCount < 50000) {
            return 7;
        }
        else if (followersCount < 100000) {
            return 8;
        }
        else if (followersCount < 1000000) {
            return 9;
        }
        else {
            return 10;
        }
    }

    public static int getFollowersCountRank(String str) {
        try {
            str = str.trim();
            Integer c = Integer.parseInt(str);
            return getFollowersCountRank(c);
        } catch (Exception e) {
            return DEFAULT_LEVEL;
        }
    }

    public static int getFollowersCountRank(int followersCount) {
        if (followersCount <= 0) {
            return 0;
        } else if (followersCount < 100) {
            return 1;
        } else if (followersCount < 1000) {
            return 2;
        } else if (followersCount < 10000) {
            return 3;
        } else if (followersCount < 100000) {
            return 4;
        } else if (followersCount < 1000000) {
            return 5;
        } else {
            return 6;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        System.out.println(getFollowersCountRank("90"));
        System.out.println("[PROGRAM] Program exited.");
    }
}

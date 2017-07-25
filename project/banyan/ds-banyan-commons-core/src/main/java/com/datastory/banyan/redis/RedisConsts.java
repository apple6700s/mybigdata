package com.datastory.banyan.redis;

public class RedisConsts {
    public static final int DFT_EXPIRE_SEC = 10 * 86400;
    public static final String PREFIX = "banyan1:etl:";

    //valid模块的前缀key
    public static final String VALID_MON_KEY = PREFIX + "valid:";

    public static final String METRICS_MON_KEY = PREFIX + "metric:";
    public static final String TASK_MON_KEY = PREFIX + "task:";
    public static final String REDIS_KEY_DELIMIT = ":";
    public static final String PARAM_REDIS_SERVER = "redis.server";
    public static final String REDIS_KEY_PREFIX_PATTERN = ".*:";
}

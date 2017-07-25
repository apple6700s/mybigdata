package com.datastory.commons3.es.utils;

import com.datastory.commons3.es.redis.RedisDao;
import org.apache.hadoop.conf.Configuration;

/**
 * com.datastory.commons3.es.utils.GetShardMeta
 *
 * @author Administrator
 * @since 2017/6/21
 */
public class GetShardMeta {
    private static String META_KEY = "ds:banyan:flush:";

    static String shardHost;
    static String hdfsDir;
    static String subDir;

    // 初始化redis，用于分布式记录 路径和host的信息
    RedisDao redisDao = RedisDao.getInstance(new Configuration());


    public String getShardHost(int toShard, String index) {
        try {
            shardHost = redisDao.hget(META_KEY + index, toShard + ".host");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return shardHost;
    }

    public String getHdfsDir(int toShard, String index) {
        try {
            hdfsDir = redisDao.hget(META_KEY + index, toShard + ".hdfsDir");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hdfsDir;
    }

    public String getSubDir(int toShard, String index) {
        try {
            subDir = redisDao.hget(META_KEY + index, toShard + ".subDir");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return subDir;
    }

}

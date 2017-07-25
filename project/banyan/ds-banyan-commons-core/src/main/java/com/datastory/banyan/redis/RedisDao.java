package com.datastory.banyan.redis;

import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * redis访问类
 *
 * @author yongjiang sugan
 */
public class RedisDao implements Serializable {

    private static Logger LOG = Logger.getLogger(RedisDao.class);

    private static final int REDIS_CONNECT_LATENCY = 10000;
    private static final int REDIS_POOL_MAXWAIT = 2000;
    private static final int REDIS_MAX_TOTAL = 100;
    private static final int REDIS_MAX_IDLE = 8;
    private static final int REDIS_TIMEOUT_MAXTRYTIMES = 3;

    private volatile static RedisDao redisDao = null;
    private String serversInfo = null;
    private int maxTryTimes = REDIS_TIMEOUT_MAXTRYTIMES;

    private Map<String, JedisPool> jedisPoolMap = new ConcurrentHashMap<String, JedisPool>();
    private Pattern keyPattern;

    private RedisDao() {
    }

    private RedisDao setServersInfo(String serversInfo) {
        this.serversInfo = serversInfo;
        return this;
    }

    private void init(Configuration conf, JedisPoolConfig jedisPoolConfig) {

        if (serversInfo == null && conf != null)
            serversInfo = conf.get(RedisConsts.PARAM_REDIS_SERVER);
        if (serversInfo == null) {
            LOG.error("no parameter " + RedisConsts.PARAM_REDIS_SERVER);
        }
        if (conf == null)
            conf = new Configuration();

        int redisConnectLatency = conf.getInt("redis.connect.latency", REDIS_CONNECT_LATENCY);
        int redisPoolMaxWait = conf.getInt("redis.pool.max.wait", REDIS_POOL_MAXWAIT);
        int redisPoolMaxTotal = conf.getInt("redis.pool.max.total", REDIS_MAX_TOTAL);
        int redisPoolMaxIdle = conf.getInt("redis.pool.max.idle", REDIS_MAX_IDLE);
        maxTryTimes = conf.getInt("redis.timeout.try.times", REDIS_TIMEOUT_MAXTRYTIMES);

        LOG.info("==serverInfo:\t" + serversInfo);
        String[] splits = serversInfo.split(StringUtil.STR_DELIMIT_1ST);
        String redis_key_prefix = "";

        for (String str : splits) {
            try {
                String[] sps = str.split(StringUtil.STR_DELIMIT_2ND);
                if (sps.length < 2) {
                    LOG.error("serversInfo error: " + str);
                    continue;
                }
                String keyPrefix = sps[0];
                redis_key_prefix = redis_key_prefix + StringUtil.DELIMIT_1ST + sps[0];
                String[] inSps = sps[1].split(RedisConsts.REDIS_KEY_DELIMIT);

                if (jedisPoolConfig == null)
                    jedisPoolConfig = new JedisPoolConfig();

                jedisPoolConfig.setMaxTotal(redisPoolMaxTotal);
                jedisPoolConfig.setMaxIdle(redisPoolMaxIdle);
                jedisPoolConfig.setMaxWaitMillis(redisPoolMaxWait);
                jedisPoolConfig.setTestOnBorrow(false);
                jedisPoolConfig.setTestOnReturn(false);

                JedisPool jedisPool;
                if (inSps.length > 2) {
                    jedisPool = new JedisPool(
                            jedisPoolConfig,
                            inSps[0],
                            Integer.parseInt(inSps[1]),
                            redisConnectLatency,
                            null,
                            Integer.parseInt(inSps[2])
                    );
                } else if (inSps.length > 3) {
                    jedisPool = new JedisPool(
                            jedisPoolConfig,
                            inSps[0],
                            Integer.parseInt(inSps[1]),
                            redisConnectLatency,
                            inSps[3],
                            Integer.parseInt(inSps[2])
                    );
                } else {
                    jedisPool = new JedisPool(
                            jedisPoolConfig,
                            inSps[0],
                            Integer.parseInt(inSps[1]),
                            redisConnectLatency
                    );
                }

                // test if validate and return now

                Jedis jedis = jedisPool.getResource();
                jedisPool.returnResource(jedis);

                jedisPoolMap.put(keyPrefix, jedisPool);
            } catch (Exception e) {
                LOG.error("cannot init redis resource , error key : " + str);
                LOG.error(e.getMessage(), e);
            }
        }

        if (redis_key_prefix.length() > 0) {
            redis_key_prefix = redis_key_prefix.substring(1);
            redis_key_prefix = "(" + redis_key_prefix + ")";
            keyPattern = Pattern.compile(redis_key_prefix);
        } else {
            LOG.error("lack of redis.server parameter");
        }
    }

    public static RedisDao getInstance(Configuration conf) {
        if (redisDao == null) {
            synchronized (RedisDao.class) {
                if (redisDao == null) {
                    redisDao = new Builder().setConf(conf).build();
                }
            }
        }
        return redisDao;
    }

    public static RedisDao getInstance(String serverInfo) {
        if (redisDao == null) {
            synchronized (RedisDao.class) {
                if (redisDao == null) {
                    redisDao = new Builder().setServerInfo(serverInfo).build();
                }
            }
        }
        return redisDao;
    }

    private String getKeyPrefix(String key) {
        if (keyPattern != null) {
            Matcher m = keyPattern.matcher(key);
            if (m.find())
                return m.group(1);
        }
        return null;
    }

    public synchronized JedisPool getJedisPool(String key) {
        String keyPrefix = getKeyPrefix(key);
        if (keyPrefix == null) {
            LOG.error("key is illegal, can not get jedisPool: " + key);
            return null;
        }
        return jedisPoolMap.get(keyPrefix);
    }

    public Pair<Pipeline, Pair<JedisPool, Jedis>> pipeline(String key) {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();
        return new Pair<Pipeline, Pair<JedisPool, Jedis>>(jedis.pipelined(),
                new Pair<JedisPool, Jedis>(pool, jedis));
    }

    public Long persist(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();
        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.persist(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String set(String key, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.set(key, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String get(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.get(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Boolean exists(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Boolean result = jedis.exists(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    /**
     * @param keys , must be same prefix
     * @return
     * @throws Exception
     */
    public Map<String, Response<Boolean>> existsPipeline(
            final List<String> keys) throws Exception {
        JedisPool pool = getJedisPool(keys.get(0));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            HashMap<String, Response<Boolean>> ret = new HashMap<>();
            try {
                Pipeline pipeline = jedis.pipelined();
                for (String key : keys) {
                    ret.put(key, pipeline.exists(key));
                }
                pipeline.sync();
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String type(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.type(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long expire(String key, int seconds) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.expire(key, seconds);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long expireAt(String key, long unixTime) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.expireAt(key, unixTime);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long ttl(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.ttl(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String getSet(String key, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.getSet(key, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String setex(String key, int seconds, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.setex(key, seconds, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long decrBy(String key, long integer) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.decrBy(key, integer);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long decr(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.decr(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long incrBy(String key, long integer) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.incrBy(key, integer);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Map<String, Response<Long>> incrByPipeline(FreqDist<String> dist) throws Exception {
        if (dist == null || dist.isEmpty()) return null;

        JedisPool pool = getJedisPool(RedisConsts.PREFIX);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        Map<String, Response<Long>> ret = new HashMap<>();
        while (tryTimes-- > 0) {
            try {
                Pipeline pipeline = jedis.pipelined();
                for (Map.Entry<String, Integer> e : dist.entrySet()) {
                    Response<Long> res = pipeline.incrBy(e.getKey(), e.getValue().longValue());
                    ret.put(e.getKey(), res);
                }
                pipeline.sync();
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Map<String, Response<Long>> incrByPipeline(List<Pair<String, Long>> dist) throws Exception {
        if (dist == null || dist.isEmpty()) return null;

        JedisPool pool = getJedisPool(RedisConsts.PREFIX);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        Map<String, Response<Long>> ret = new HashMap<>();
        while (tryTimes-- > 0) {
            try {
                Pipeline pipeline = jedis.pipelined();
                for (Pair<String, Long> e : dist) {
                    Response<Long> res = pipeline.incrBy(e.getFirst(), e.getSecond());
                    ret.put(e.getFirst(), res);
                }
                pipeline.sync();
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }


    public Long incr(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.incr(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long append(String key, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.append(key, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String substr(String key, int start, int end) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.substr(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    // hashes api
    public Long hset(String key, String field, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.hset(key, field, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String hget(String key, String field) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.hget(key, field);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long hsetnx(String key, String field, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.hsetnx(key, field, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long setnx(String key, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.setnx(key, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String hmset(String key, Map<String, String> hash) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.hmset(key, hash);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public List<String> hmget(String key, String... fields) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                List<String> result = jedis.hmget(key, fields);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long hincrBy(String key, String field, long value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.hincrBy(key, field, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Boolean hexists(String key, String field) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Boolean result = jedis.hexists(key, field);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long del(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.del(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long del(String... key) throws Exception {
        JedisPool pool = getJedisPool(key[0]);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.del(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long hdel(String key, String... fields) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.hdel(key, fields);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long hlen(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.hlen(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> hkeys(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.hkeys(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public List<String> hvals(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                List<String> result = jedis.hvals(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Map<String, String> hgetAll(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Map<String, String> result = jedis.hgetAll(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    // list api
    public Long rpush(String key, String... strings) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.rpush(key, strings);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long lpush(String key, String... strings) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.lpush(key, strings);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long lpushx(String key, String string) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.lpushx(key, string);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long llen(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.llen(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public List<String> lrange(String key, long start, long end)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                List<String> result = jedis.lrange(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String ltrim(String key, long start, long end) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.ltrim(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String lindex(String key, long index) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.lindex(key, index);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String lset(String key, long index, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.lset(key, index, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long lrem(String key, long count, String value) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.lrem(key, count, value);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String lpop(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.lpop(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String rpop(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.rpop(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    // set api
    public Long sadd(String key, String... members) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.sadd(key, members);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> smembers(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.smembers(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long srem(String key, String... members) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.srem(key, members);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String spop(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.spop(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long scard(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.scard(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Boolean sismember(String key, String member) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Boolean result = jedis.sismember(key, member);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public boolean[] sismemberPipeline(String key, String... members)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Pipeline pipeline = jedis.pipelined();
                for (String member : members) {
                    pipeline.sismember(key, member);
                }
                List<Object> retObjects = pipeline.syncAndReturnAll();
                boolean[] ret = new boolean[retObjects.size()];
                for (int i = 0; i < retObjects.size(); i++) {
                    ret[i] = (Boolean) retObjects.get(i);
                }
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String srandmember(final String key) throws Exception {
//        JedisPool pool = getJedisPool(key);
//        if (pool == null)
//            return null;
//        Jedis jedis = pool.getResource();
//
//        int tryTimes = maxTryTimes;
//        while (tryTimes-- > 0) {
//            try {
//                String result = jedis.srandmember(key);
//                pool.returnResource(jedis);
//                return result;
//            } catch (Exception e) {
//                if (e.getMessage().contains("SocketTimeoutException")
//                        && tryTimes > 0) {
//                    continue;
//                }
//                pool.returnBrokenResource(jedis);
//                throw e;
//            }
//        }
//        return null;
        Object tmp = retry(key, new Action() {
            @Override
            public Object handle(Jedis jedis) {
                return jedis.srandmember(key);
            }
        });
        return (tmp != null) ? (String) tmp : null;
    }

    public ScanResult<String> sscan(String key, String cursor, ScanParams params) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                ScanResult<String> result = jedis.sscan(key, cursor, params);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    // sort set api
    public Long zcard(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zcard(key);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long zadd(String key, double score, String member) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zadd(key, score, member);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long zadd(String key, Map<String, Double> scoreMembers)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zadd(key, scoreMembers);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long zadd(final byte[] key, final double score, final byte[] members)
            throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zadd(key, score, members);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> zrange(String key, long start, long end)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.zrange(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Double zincrby(String key, double score, String member)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Double result = jedis.zincrby(key, score, member);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String zfirst(String key) throws Exception {
        Set<String> res = zrange(key, 0L, 0L);
        if (res == null || res.isEmpty()) {
            return null;
        }
        for (String s : res) {
            return s;
        }
        return null;
    }

    public String zget(String key, Long rank) throws Exception {
        Set<String> res = zrange(key, rank, rank);
        if (res == null || res.isEmpty()) {
            return null;
        }
        for (String s : res) {
            return s;
        }
        return null;
    }

    public Long zrem(String key, String... members) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zrem(key, members);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long zrem(final byte[] key, final byte[]... members)
            throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zrem(key, members);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long zremrangebyrank(String key, long start, long end)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zremrangeByRank(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long zrank(String key, String member) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zrank(key, member);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<byte[]> zrange(final byte[] key, final int start, final int end)
            throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<byte[]> result = jedis.zrange(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min,
                                     final double max) throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<byte[]> result = jedis.zrangeByScore(key, min, max);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<byte[]> zrangeByScore(final byte[] key, final double min,
                                     final double max, final int offset, final int count)
            throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<byte[]> result = jedis.zrangeByScore(key, min, max, offset, count);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double min,
                                        final double max) throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<byte[]> result = jedis.zrevrangeByScore(key, min, max);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Long zrevrank(String key, String member) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zrevrank(key, member);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<byte[]> zrevrange(final byte[] key, final int start,
                                 final int end) throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<byte[]> result = jedis.zrevrange(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> zrevrange(String key, long start, long end)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.zrevrange(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<Tuple> zrangeWithScores(String key, long start, long end)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<Tuple> result = jedis.zrangeWithScores(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<Tuple> zrevrangeWithScores(String key, long start, long end)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<Tuple> result = jedis.zrevrangeWithScores(key, start, end);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> zrangeByScore(final String key, final double min,
                                     final double max) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.zrangeByScore(key, min, max);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> zrangeByScore(final String key, final double min,
                                     final double max, final int offset, final int count)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> reuslt = jedis.zrangeByScore(key, min, max, offset, count);
                pool.returnResource(jedis);
                return reuslt;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> zrevrangeByScore(final String key, final double min,
                                        final double max) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.zrevrangeByScore(key, min, max);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> zrevrangeByScore(final String key, final double min,
                                        final double max, final int offset, final int count)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.zrevrangeByScore(key, min, max, offset, count);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<byte[]> zrevrangeByScore(final byte[] key, final double min,
                                        final double max, final int offset, final int count)
            throws Exception {
        JedisPool pool = getJedisPool(new String(key));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<byte[]> result = jedis.zrevrangeByScore(key, min, max, offset, count);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Double zscore(String key, String member) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Double result = jedis.zscore(key, member);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Double[] zscorePipeline(String key, String... members)
            throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Pipeline pipeline = jedis.pipelined();
                for (String member : members) {
                    pipeline.zscore(key, member);
                }
                List<Object> retObjects = pipeline.syncAndReturnAll();
                Double[] ret = new Double[retObjects.size()];
                for (int i = 0; i < retObjects.size(); i++) {
                    Object obj = retObjects.get(i);
                    if (obj != null)
                        ret[i] = (Double) obj;
                }
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    /**
     * union a list of sort map. result elements will be save in dstkey
     *
     * @param dstkye
     * @param sets
     * @return the number of elements in the resulting set at destination
     * @throws Exception
     */
    public Long zunionstore(String dstkye, String... sets) throws Exception {
        JedisPool pool = getJedisPool(dstkye);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zunionstore(dstkye, sets);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                pool.returnBrokenResource(jedis);
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                throw e;
            }
        }
        return null;
    }

    /**
     * intersect a list of sort map. result elements will be saved in dstkey
     *
     * @param dstkey
     * @param sets
     * @return the number of elements in the resulting set at destination
     * @throws Exception
     */
    public Long zinterstore(String dstkey, String... sets) throws Exception {
        JedisPool pool = getJedisPool(dstkey);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.zinterstore(dstkey, sets);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    /**
     * all keys must be the same key prefix
     *
     * @param
     * @return
     * @throws Exception
     */
    public List<String> brpop(int timeout, String... keys) throws Exception {
        if (keys == null)
            return null;
        String key = keys[0];
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                List<String> result = jedis.brpop(timeout, keys);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    /**
     * source and destination must be the same prefix key
     *
     * @param
     * @return
     * @throws Exception
     */
    public String brpoplpush(String source, String destination, int timeout)
            throws Exception {
        JedisPool pool = getJedisPool(source);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.brpoplpush(source, destination, timeout);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public Set<String> keys(final String prefixPattern) throws Exception {
        JedisPool pool = getJedisPool(prefixPattern);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Set<String> result = jedis.keys(prefixPattern);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public List<Response<String>> hmsetPipeline(
            final List<Pair<String, Map<String, String>>> kvList)
            throws Exception {
        JedisPool pool = getJedisPool(kvList.get(0).getFirst());
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            List<Response<String>> ret = new LinkedList<Response<String>>();
            try {
                Pipeline pipeline = jedis.pipelined();
                for (Pair<String, Map<String, String>> kv : kvList) {
                    ret.add(pipeline.hmset(kv.getFirst(), kv.getSecond()));
                }
                pipeline.sync();
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public List<Response<Long>> hincrByPipeline(String mKey, final List<Pair<String, Long>> kvs)
            throws Exception {
        JedisPool pool = getJedisPool(mKey);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            List<Response<Long>> ret = new LinkedList<>();
            try {
                Pipeline pipeline = jedis.pipelined();
                for (Pair<String, Long> kv : kvs) {
                    ret.add(pipeline.hincrBy(mKey, kv.getFirst(), kv.getSecond()));
                }
                pipeline.sync();
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public List<Response<String>> setPipeline(
            final List<Pair<String, String>> list) throws Exception {
        JedisPool pool = getJedisPool(list.get(0).getFirst());
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            List<Response<String>> ret = new LinkedList<Response<String>>();
            try {
                Pipeline pipeline = jedis.pipelined();
                for (Pair<String, String> kv : list) {
                    ret.add(pipeline.set(kv.getFirst(), kv.getSecond()));
                }
                pipeline.sync();
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    /**
     * @param keys , must be same prefix
     * @return
     * @throws Exception
     */
    public List<Pair<String, Response<String>>> getPipeline(
            final List<String> keys) throws Exception {
        JedisPool pool = getJedisPool(keys.get(0));
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            List<Pair<String, Response<String>>> ret = new LinkedList<Pair<String, Response<String>>>();
            try {
                Pipeline pipeline = jedis.pipelined();
                for (String key : keys) {
                    ret.add(new Pair<String, Response<String>>(key, pipeline
                            .get(key)));
                }
                pipeline.sync();
                pool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public String rename(String oldkey, String newkey) throws Exception {
        JedisPool pool = getJedisPool(oldkey);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                String result = jedis.rename(oldkey, newkey);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    public List<String> sort(String key) {
        JedisPool pool = getJedisPool(key);
        if (pool == null)
            return null;
        Jedis jedis = pool.getResource();
        List<String> sortedValues = null;
        try {
            sortedValues = jedis.sort(key);
            pool.returnResource(jedis);

        } catch (Exception e) {
            LOG.error(key, e);
            pool.returnBrokenResource(jedis);
        }

        return sortedValues;
    }

//    public long smove(String source, String destination, String value)
//            throws Exception {
//        JedisPool pool = getJedisPool(source);
//        if (pool == null)
//            return -1;
//        Jedis jedis = pool.getResource();
//
//        int tryTimes = maxTryTimes;
//        while (tryTimes-- > 0) {
//            try {
//                Long result = jedis.smove(source, destination, value);
//                pool.returnResource(jedis);
//                return result;
//            } catch (Exception e) {
//                if (e.getMessage().contains("SocketTimeoutException")
//                        && tryTimes > 0) {
//                    continue;
//                }
//                pool.returnBrokenResource(jedis);
//                throw e;
//            }
//        }
//        return -1;
//    }


    //For test
    public long smove(final String source, final String destination, final String value)
            throws Exception {
        Object tmp = retry(source, new Action() {
            @Override
            public Object handle(Jedis jedis) {
                return jedis.smove(source, destination, value);
            }
        });
        return tmp != null ? Long.parseLong(tmp + "") : -1;
    }

    private Object retry(String source, Action handler) {
        JedisPool pool = getJedisPool(source);
        if (pool == null)
            return -1;
        Jedis jedis = pool.getResource();

        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Object result = handler.handle(jedis);
                pool.returnResource(jedis);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                pool.returnBrokenResource(jedis);
                throw e;
            }
        }
        return null;
    }

    /**
     * Builder
     */
    public static class Builder {
        private Configuration conf;
        private String serverInfo;
        private JedisPoolConfig jedisPoolConfig;

        public Builder setConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public Builder setServerInfo(String serverInfo) {
            this.serverInfo = serverInfo;
            return this;
        }

        public Builder setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
            this.jedisPoolConfig = jedisPoolConfig;
            return this;
        }

        public RedisDao build() {
            RedisDao redisDao = new RedisDao();
            if (serverInfo != null)
                redisDao.setServersInfo(serverInfo);
            redisDao.init(conf, jedisPoolConfig);
            return redisDao;
        }
    }


    //------- add by able.chan

    /**
     * 获取jedis示例
     *
     * @param key
     * @return
     * @throws Exception
     */
    public Jedis getJedis(String key) throws Exception {
        JedisPool pool = getJedisPool(key);
        if (pool == null) {
            throw new NullPointerException("getting jedis failed !!!");
        }
        return pool.getResource();
    }

    /**
     * 获取jedis示例
     *
     * @param key
     * @param jedis
     * @return
     */
    public boolean returnJedis(String key, Jedis jedis) {
        JedisPool pool = getJedisPool(key);
        if (pool == null) {
            throw new NullPointerException("getting jedis failed !!!");
        }
        pool.returnResource(jedis);
        return true;
    }

    /**
     * @param jedis
     * @param key
     * @param field
     * @param value
     * @return
     * @throws Exception
     */
    public Long hincrBy(Jedis jedis, String key, String field, long value) throws Exception {
        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Long result = jedis.hincrBy(key, field, value);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                throw e;
            }
        }
        return null;
    }

    public Response<Long> hincrBy(Pipeline pipeline, String key, String field, long value) throws Exception {
        int tryTimes = maxTryTimes;
        while (tryTimes-- > 0) {
            try {
                Response<Long> result = pipeline.hincrBy(key, field, value);
                return result;
            } catch (Exception e) {
                if (e.getMessage().contains("SocketTimeoutException")
                        && tryTimes > 0) {
                    continue;
                }
                throw e;
            }
        }
        return null;
    }
}


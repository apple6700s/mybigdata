package com.datastory.banyan.redis;

import redis.clients.jedis.Jedis;

/**
 * @author sugan
 * @since 2016-08-11.
 */
interface Action {
    Object handle(Jedis jedis);
}

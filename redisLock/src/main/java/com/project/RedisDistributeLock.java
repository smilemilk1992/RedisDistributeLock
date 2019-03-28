package com.project;

import redis.clients.jedis.Jedis;

/**
 * @author haochen
 * @date 2019/3/28 9:59 AM
 */
public interface RedisDistributeLock {

    /**
     * 公平锁， 只能说 本机的公平
     * @param jedis
     * @param key
     * @param uuid
     */
    void fairLock(Jedis jedis, String key, String uuid);

    /**
     * 非公平锁
     * @param jedis
     * @param key
     * @param uuid
     */
    void unfairLock(Jedis jedis, String key, String uuid);

    /**
     * 锁 默认非公平
     * @param jedis
     * @param key
     * @param uuid
     */
    void lock(Jedis jedis, String key, String uuid);

    /**
     * 解锁
     * @param jedis
     * @param key
     * @param uuid
     */
    void release(Jedis jedis, String key, String uuid);
}


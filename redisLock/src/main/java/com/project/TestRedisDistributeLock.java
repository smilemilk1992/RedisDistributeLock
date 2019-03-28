package com.project;


import redis.clients.jedis.Jedis;

import java.util.UUID;

/**
 * @author haochen
 * @date 2019/3/28 10:04 AM
 */
public class TestRedisDistributeLock {

    public static void main(String[] args) {
        for (int i = 1; i < 10; i ++) {
            new TestLock().start();
        }
    }

    static class TestLock extends Thread {

        static RedisDistributeLock locker = new DefaultRedisDistributeLock();
        String identifier = UUID.randomUUID().toString();


        @Override
        public void run() {
            Jedis jedis = RedisClient.getJedis();
            //加锁
            locker.lock(jedis, "test1", identifier);

            // TODO 模拟业务
            System.out.println(Thread.currentThread().getName() + " "+System.currentTimeMillis()+" get lock....."+" uuid="+identifier);
            try {
                Thread.sleep(900);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + " "+System.currentTimeMillis()+ " release lock....."+" uuid="+identifier);

            //解锁
            locker.release(jedis, "test1", identifier);
        }
    }
}


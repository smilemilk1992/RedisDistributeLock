package com.project;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.Set;


/**
 * @author chenhao
 */
public class RedisClient {
    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);
    private static final String FAILED_SETKEY = "redis set failed,key =";
    private static final String FAILED_SETEXPEIR = "redis set expiretime failed,key =";
    private static final String NULL_KEY = "redis key must not be null";
    public static JedisPool jedisSentinelPool;
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        // 设置最大连接数
	            config.setMaxWaitMillis(2000);
        // 设置最大空闲数
	            config.setMaxTotal(50);
        // 设置最大等待时间
                config.setMaxWaitMillis(1000 * 100);
	            config.setTestOnBorrow(true);
        jedisSentinelPool = new JedisPool(config, "127.0.0.1", 6379, 2000, "123456");
    }
    private RedisClient() {
    }

    /**
     * 包装get命令
     *
     * @param key 待查询的key，非空
     * @return String
     */
    public static String get(String key) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.get(key);
        } catch (Exception e) {
            logger.error("redis get failed,key =" + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return StringUtils.EMPTY;
    }

    /**
     * 包装set命令
     *
     * @param key   key
     * @param value value
     * @return boolean
     */
    public static boolean set(String key, String value) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            String ret = jedis.set(key, StringUtils.trimToEmpty(value));
            return "ok".equalsIgnoreCase(ret);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     * 包装待失效时间的set命令
     *
     * @param key   key
     * @param value value
     * @return boolean
     */
    public static boolean set(String key, String value, int seconds) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            String ret = jedis.set(key, StringUtils.trimToEmpty(value));
            jedis.expire(key, seconds);
            return "ok".equalsIgnoreCase(ret);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     * 封装del命令
     *
     * @param key 待删除的key
     * @return boolean
     */
    public static boolean del(String key) {
        Assert.hasLength(key, NULL_KEY);
        Long removedSize = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            removedSize = jedis.del(key);
        } catch (Exception e) {
            logger.error("redis del failed,key = " + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return removedSize > 0;
    }

    public static Long hset( String key,  String field,  String value) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return  jedis.hset(key, field,value);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return 2L;
    }

    /**
     * 读取hashmap的元素个数
     * */
    public static Long hlen(final String key){
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return  jedis.hlen(key);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return 2L;
    }

    /**
     * 包装hmset命令
     * @param key
     * @param map
     * @return
     */
    public static boolean hmset(String key, Map<String,String> map,int seconds) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            String ret = jedis.hmset(key, map);
            jedis.expire(key, seconds);
            return "ok".equalsIgnoreCase(ret);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     *  包装hmget命令
     * @param key1
     * @param key2
     * @return
     */
    public static String hmget(String key1,String key2) {
        Assert.hasLength(key1, NULL_KEY);
        Assert.hasLength(key2, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.hget(key1, key2);
        } catch (Exception e) {
            logger.error("redis get failed,key =" + key1, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return StringUtils.EMPTY;
    }

    /**
     * 包装hdel命令
     * @param key
     * @param fields
     * @return
     */
    public static boolean hdel(String key,String ... fields) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            ret = jedis.hdel(key, fields);
        } catch (Exception e) {
            logger.error("redis del failed,key =" + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret>0L;
    }

    /**
     * 包装hgetAll命令
     * @param key
     * @return
     */
    public static Map<String, String> hgetAll(String key) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return MapUtils.EMPTY_MAP;
    }

    /**
     * 包装zadd命令
     * @param key
     * @param score
     * @param member
     * @return
     */
    public static boolean zadd(String key, double score, String member, int seconds) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long ret = jedis.zadd(key, score, member);
            jedis.expire(key, seconds);
            return 1==ret;
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     * 包装zincrby命令
     * @param key
     * @param score
     * @param member
     * @return
     */
    public static double zincrby(String key, double score, String member) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Double ret = jedis.zincrby(key, score, member);
            return ret;
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return 0.0;
    }
    /**
     * 包装zrange命令
     * @param key
     * @param start
     * @param end
     * @return
     */
    public static Set<String> zrange(String key, Long start, Long end) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.zrange(key, start, end);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return SetUtils.EMPTY_SET;
    }

    /**
     * 包装zrevrange命令
     * @param key
     * @param start
     * @param end
     * @return
     */
    public static Set<String> zrevrange(String key, Long start, Long end) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.zrevrange(key, start, end);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return SetUtils.EMPTY_SET;
    }

    /**
     * 包装zrem命令
     * @param key
     * @param str
     * @return
     */
    public static boolean  zrem(String key, String str) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            ret = jedis.zrem(key, str);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret>0L;
    }

    /**
     * 包装zremrangeByRank命令
     * @param start
     * @param end
     * @return
     */
    public static Long  zremrangeByRank(String key, Long start,Long end) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            ret =jedis.zremrangeByRank(key, start, end);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

    /**
     * 包装sadd命令
     * @param key
     * @param str ...
     * @return
     */
    public static boolean  sadd(String key, int seconds ,String... str) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            ret = jedis.sadd(key, str);
            jedis.expire(key, seconds);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret>0L;
    }


    public static Set<String> getSadd(String key){
        Jedis jedis = jedisSentinelPool.getResource();
        return  jedis.zrange(key,0,1);
    }

    /**
     * 包装smembers命令
     * @param key
     * @param key
     * @return
     */
    public static Set<String> smembers(String key ) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.smembers(key );
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return SetUtils.EMPTY_SET;
    }

    /**
     * 包装srem命令
     * @param key
     * @param str
     * @return
     */
    public static boolean  srem(String key, String... str) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            ret = jedis.srem(key, str);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret>0L;
    }

    /**
     * 返回集合的排名命令 -1
     * @param key
     * @param member
     * @return
     */
    public static Long zrank(String key, String member) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long zrankResult=jedis.zrank(key, member);
            if (zrankResult!=null){
                ret =zrankResult.longValue();
            }else {
                ret=-1;
            }
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

    /**
     * 包装zadd命令 批量添加
     * @param key
     * @param scoreMembers
     * @param seconds
     * @return
     */
    public static boolean zadd(String key, Map<String, Double> scoreMembers, int seconds) {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long ret = jedis.zadd(key, scoreMembers);
            jedis.expire(key, seconds);
            return ret>0;
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     * 统计总个数
     * @param key
     * @return
     */
    public static Long zcard(String key) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long zrankResult=jedis.zcard(key);
            if (zrankResult!=null){
                ret =zrankResult.longValue();
            }else {
                ret=0;
            }
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

    /**
     * 移除有序集 key 中，指定排名(rank)区间内的所有成员
     * @param key
     * @param start
     * @param stop
     * @return
     */
    public static Long zremrangeByRank(String key, long start, long stop) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long zrankResult=jedis.zremrangeByRank(key,start, stop);
            if (zrankResult!=null){
                ret =zrankResult.longValue();
            }else {
                ret=0;
            }
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

    /**
     * 字符数据累加
     * @param key
     * @return
     */
    public static Long incr(String key) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long zrankResult=jedis.incr(key);
            if (zrankResult!=null){
                ret =zrankResult.longValue();
            }else {
                ret=0;
            }
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }

    /**
     * 字符数据递减
     * @param key
     * @return
     */
    public static Long decr(String key) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long zrankResult=jedis.decr(key);
            if (zrankResult!=null){
                ret =zrankResult.longValue();
            }else {
                ret=0;
            }
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

        return ret;
    }

    /**
     * 重新设置过期时间
     * */
    public static Long expire( String key,  int seconds)
    {
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            logger.error(FAILED_SETEXPEIR + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return 2L;
    }

    /**
     * 字符数据递减指定数
     * @param key
     * @param number
     * @return
     */
    public static Long decrBy(String key,Integer number) {
        Assert.hasLength(key, NULL_KEY);
        long ret = 0L;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            Long zrankResult=jedis.decrBy(key,number);
            if (zrankResult!=null){
                ret =zrankResult.longValue();
            }else {
                ret=0;
            }
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return ret;
    }
    /**
     * 包装exists命令
     * @param key
     * @return
     */
    public static boolean  exists(String key) {
        Assert.hasLength(key, NULL_KEY);
        boolean exists=false;
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            exists = jedis.exists(key);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return exists;
    }

    /**
     * 包装keys命令
     * @param pattern
     * @return
     */
    public static Set<String> keys(String pattern) {
        Assert.hasLength(pattern, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.keys(pattern);
        } catch (Exception e) {
            logger.error(FAILED_SETKEY + pattern, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return SetUtils.EMPTY_SET;
    }

    /**
     * 调用此方法  getJedis ，切记使用 closeJedis(Jedis jedis) 进行关闭
     * Jedis 获取，用于事务
     * @return Jedis实体
     */
    public static Jedis getJedis() {
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis;
        } catch (Exception e) {
            logger.error("redis get failed, message:{} e:{}", e);
        }
        return null;
    }

    /**
     * Jedis 关闭
     */
    public static boolean closeJedis(Jedis jedis) {
        logger.info("redis close ...");
        try {
            if (jedis != null) {
                jedis.close();
            }
            logger.info("close redis success");
            return true;
        } catch (Exception e) {
            logger.error("redis close failed, message:{} e:{}",e.getMessage(),e);
        }
        return false;
    }

    /**
     *  弹出操作,单个
     *  jedis pop
     */
    public static String getPop( String key){
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.lpop(key);
        } catch (Exception e) {
            logger.error("redis get failed,key =" + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    public static boolean lpush(String key, int seconds,String...request){
        Assert.hasLength(key, NULL_KEY);
        Jedis jedis = null;
        long ret = 0L;
        try {
            jedis = jedisSentinelPool.getResource();
            ret =  jedis.lpush(key,request);
            jedis.expire(key, seconds);
            return ret>0;
        } catch (Exception e) {
            logger.error("redis get failed,key =" + key, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }
    public static String lpop(String key){
        Assert.hasLength(key,NULL_KEY);
        Jedis jedis  =  null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.lpop(key);
        }catch (Exception e){
            logger.error("error"+e);
        }finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return key;
    }
    public static String rpop(String key){
        Assert.hasLength(key,NULL_KEY);
        Jedis jedis  =  null;
        try {
            jedis = jedisSentinelPool.getResource();
            return jedis.rpop(key);
        }catch (Exception e){
            logger.error("error"+e);
        }finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return key;
    }

    /**
     *  4s 有效
     * @param key
     * @param value
     * @return
     */
    public static Boolean setnx(String key,String value){
        Assert.hasLength(key,NULL_KEY);
        Assert.hasLength(value,NULL_KEY);
        Jedis jedis  =  null;
        long res = 0L;
        try {
            jedis = jedisSentinelPool.getResource();
            res = jedis.setnx(key,value);
            jedis.expire(key, 4);
            return res > 0 ;
        }catch (Exception e){
            logger.error("error" + e );
        }finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

}

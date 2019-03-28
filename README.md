&ensp;&ensp;&ensp;&ensp; 分布式锁一般有三种实现方式：1. 数据库乐观锁；2. 基于Redis的分布式锁；3. 基于ZooKeeper的分布式锁。前面已经介绍了[redis的分布式锁的实现](https://smilemilk1992.github.io/2019/01/09/java%E5%88%86%E5%B8%83%E5%BC%8F%E9%94%81/)，但是本章介绍另外一种基于redis分布式锁的实现。

***

# 1、可靠性
首先，为了确保分布式锁可用，我们至少要确保锁的实现同时满足以下四个条件：

* 互斥性。在任意时刻，只有一个客户端能持有锁。
* 不会发生死锁。即使有一个客户端在持有锁的期间崩溃而没有主动解锁，也能保证后续其他客户端能加锁。
* 具有容错性。只要大部分的Redis节点正常运行，客户端就可以加锁和解锁。
* 解铃还须系铃人。加锁和解锁必须是同一个客户端，客户端自己不能把别人加的锁给解了。

# 2、代码实现

** 组件依赖 **

```bash
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>2.9.0</version>
</dependency>
```


** 加锁 & 释放锁代码 **
```java
public class RedisTool {
    private static final Long RELEASE_SUCCESS = 1L;

    private static final String LOCK_SUCCESS = "OK";
    private static final String SET_IF_NOT_EXIST = "NX";
    private static final String SET_WITH_EXPIRE_TIME = "PX";

    /**
     * 尝试获取分布式锁
     *
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @param expireTime 超期时间
     * @return 是否获取成功
     * **/
    public static boolean tryGetDistributedLock(Jedis jedis, String lockKey, String requestId, int expireTime) {

        String result = jedis.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime);

        if (LOCK_SUCCESS.equals(result)) {
            return true;
        }
        return false;

    }

    /**
     * 释放分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @return 是否释放成功
     */
    public static boolean releaseDistributedLock(Jedis jedis, String lockKey, String requestId) {

        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));

        if (RELEASE_SUCCESS.equals(result)) {
            return true;
        }
        return false;

    }


}
```

** 加锁说明 **

&ensp;&ensp;&ensp;&ensp;可以看到，我们加锁就一行代码：jedis.set(String key, String value, String nxxx, String expx, int time)，这个set()方法一共有五个形参：

* 第一个为key，我们使用key来当锁，因为key是唯一的。

* 第二个为value，我们传的是requestId，很多童鞋可能不明白，有key作为锁不就够了吗，为什么还要用到value？原因就是我们在上面讲到可靠性时，分布式锁要满足第四个条件解铃还须系铃人，通过给value赋值为requestId，我们就知道这把锁是哪个请求加的了，在解锁的时候就可以有依据。requestId可以使用UUID.randomUUID().toString()方法生成。

* 第三个为nxxx，这个参数我们填的是NX，意思是SET IF NOT EXIST，即当key不存在时，我们进行set操作；若key已经存在，则不做任何操作；

* 第四个为expx，这个参数我们传的是PX，意思是我们要给这个key加一个过期的设置，具体时间由第五个参数决定。

* 第五个为time，与第四个参数相呼应，代表key的过期时间。


总的来说，执行上面的set()方法就只会导致两种结果：
1. 当前没有锁（key不存在），那么就进行加锁操作，并对锁设置个有效期，同时value表示加锁的客户端。
2. 已有锁存在，不做任何操作。

&ensp;&ensp;&ensp;&ensp;我们的加锁代码满足我们可靠性里描述的三个条件。首先，set()加入了NX参数，可以保证如果已有key存在，则函数不会调用成功，也就是只有一个客户端能持有锁，满足互斥性。其次，由于我们对锁设置了过期时间，即使锁的持有者后续发生崩溃而没有解锁，锁也会因为到了过期时间而自动解锁（即key被删除），不会发生死锁。最后，因为我们将value赋值为requestId，代表加锁的客户端请求标识，那么在客户端在解锁的时候就可以进行校验是否是同一个客户端。

** 解锁说明 **

&ensp;&ensp;&ensp;&ensp;我们写了一个简单的Lua脚本代码，我们将Lua代码传到jedis.eval()方法里，并使参数KEYS[1]赋值为lockKey，ARGV[1]赋值为requestId。eval()方法是将Lua代码交给Redis服务端执行。

&ensp;&ensp;&ensp;&ensp;Lua代码的功能是首先获取锁对应的value值，检查是否与requestId相等，如果相等则删除锁（解锁），确保上述操作是原子性的。


代码运行效果：
```bash
Thread-4 1553754545611 get lock..... uuid=cb1964bc-cf8d-4e62-8219-7a36cda260e8
Thread-4 1553754546511 release lock..... uuid=cb1964bc-cf8d-4e62-8219-7a36cda260e8
Thread-5 1553754546522 get lock..... uuid=8f7c1f6f-b884-4abc-8416-904045836318
Thread-5 1553754547425 release lock..... uuid=8f7c1f6f-b884-4abc-8416-904045836318
Thread-8 1553754547433 get lock..... uuid=440e4943-0060-495c-a440-60b8e1ddc387
Thread-8 1553754548336 release lock..... uuid=440e4943-0060-495c-a440-60b8e1ddc387
Thread-1 1553754548344 get lock..... uuid=e0d46bbd-ec87-48ce-a7e6-03af32045fe9
Thread-1 1553754549247 release lock..... uuid=e0d46bbd-ec87-48ce-a7e6-03af32045fe9
Thread-6 1553754549256 get lock..... uuid=d0db9ee9-c33f-43ce-bdfb-7251023fb51d
Thread-6 1553754550158 release lock..... uuid=d0db9ee9-c33f-43ce-bdfb-7251023fb51d
Thread-7 1553754550166 get lock..... uuid=1235940b-d059-4a09-8712-22fe00c0e647
Thread-7 1553754551070 release lock..... uuid=1235940b-d059-4a09-8712-22fe00c0e647
Thread-0 1553754551079 get lock..... uuid=d202c310-456b-4513-b839-588e05e8caae
Thread-0 1553754551983 release lock..... uuid=d202c310-456b-4513-b839-588e05e8caae
Thread-2 1553754551996 get lock..... uuid=a1c15151-3a5e-4fc4-899e-61c826bf8404
Thread-2 1553754552900 release lock..... uuid=a1c15151-3a5e-4fc4-899e-61c826bf8404
Thread-3 1553754552912 get lock..... uuid=356ecf5c-117f-493a-98b7-b360fe57afc7
Thread-3 1553754553814 release lock..... uuid=356ecf5c-117f-493a-98b7-b360fe57afc7
```
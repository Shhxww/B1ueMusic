package util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @基本功能:
 * @program:B1ueMusic
 * @author: B1ue
 * @createTime:2025-05-15 12:10:31
 **/

public class RedisUtil {
//    设置一个Redis线程池
    private final static JedisPool pool;
//    对线程池进行初始化
    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config, "node1", 6379);
    }

//    获取Jedis同步连接
    public static Jedis getJedis() {
        Jedis jedis = pool.getResource();
        jedis.select(4); // 直接选择 4 号库

        return jedis;
    }

    /**
     * 从 redis 读取维度数据
     *
     * @param jedis     jedis 对象
     * @param tableName 表名
     * @param id        维度的 id 值
     * @return 这条维度组成的 JSONObject 对象
     */
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        String jsonStr = jedis.get(key);
        if (jsonStr != null) {
            return JSON.parseObject(jsonStr);
        }
        return null;
    }

    private static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }

    /**
     * 向Redis写入数据
     * @param jedis Jedis连接
     * @param tableName 写入的表名
     * @param id    写入的key
     * @param dim   写入的value
     */
    public static void write(Jedis jedis, String tableName, String id, JSONObject dim) {
        jedis.setex(getKey(tableName, id), 24 * 60 * 60, dim.toJSONString());
    }

    public static void delete(Jedis jedis, String tableName, String id) {
        jedis.del(getKey(tableName, id));
    }

//    关闭jedis连接
    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();  // 如果 jedis 客户端是 new Jedis()得到的,则是关闭客户端.如果是通过连接池得到的,则归还
        }
    }


//--------------------------------------------------------------------------------------------



    /**
 * 获取到 redis 的异步连接
 * @return 异步链接对象
 */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
    RedisClient redisClient = RedisClient.create("redis://node1:6379/2");
    System.out.println("创建了一个redis异步连接");
    return redisClient.connect();
}

    /**
     * 关闭 redis 的异步连接
     * @param redisAsyncConn
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
    if (redisAsyncConn != null) {
        redisAsyncConn.close();
        System.out.println("关闭了一个redis异步连接");
    }
}

    /**
 * 异步的方式从 redis 读取维度数据
 * @param redisAsyncConn 异步连接
 * @param key key 的值
 * @return 读取到维度数据,封装的 json 对象中
 */
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> redisAsyncConn, String key) {
    RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();

    try {
//        读取缓存中的数据
        String json = asyncCommand.get(key).get();
//        若读到了就直接返回，若转换失败就直接跳过报错，返回空值
        if (json != null) {
            return JSON.parseObject(json);
        }
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
    return null;
}


    /**
     * 把维度数据的写入到 Redis中
     * @param redisAsyncConn    redis 的异步连接
     * @param key   rowkey  行键
     * @param dim   要写入的维度数据
     * @param ttl   保留时间
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn, String key, JSONObject dim,Long ttl) {
        // 1. 得到异步命令
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();

        // 2. 写入并设置 ttl
        asyncCommand.setex(key, ttl, dim.toJSONString());

    }



}

package org.pcchen.wordcount;

import backtype.storm.task.TopologyContext;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * 连接redis保存数据的类
 * Created by ceek on  2017-03-02 10:37
 */
public class JedisUtil {
    private static Jedis jedis;
    static {
        jedis = new Jedis("hadoop01", 6379);

    }

    public static void save(String key, String value) {
        jedis.set(key, value);
        jedis.close();
    }
    public static void save(String key, Map value) {
        jedis.hmset(key, value);
        jedis.close();
    }
}
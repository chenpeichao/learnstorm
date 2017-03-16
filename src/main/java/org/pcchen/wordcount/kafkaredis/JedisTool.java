package org.pcchen.wordcount.kafkaredis;


import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * 得到jedis的工具类
 * Created by ceek on  2017-03-16 10:59
 */
public class JedisTool {
    private static Jedis jedis = new Jedis();
    static {
        jedis = new Jedis("hadoop01", 6379);
    }

    public static Jedis getJedis() {
        return jedis;
    }

//    public static void main(String[] args) {
//        Jedis jedis1 = JedisTool.getJedis();
//        jedis.set("test", "1");
//    }
}

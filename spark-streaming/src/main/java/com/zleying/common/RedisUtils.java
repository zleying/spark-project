package com.zleying.common;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {

    private static JedisPool pool=null;

    public static synchronized Jedis getJedis(){

        if (pool==null) {
            JedisPoolConfig jPoolConfig=new JedisPoolConfig();
            //设置连接池中最大空闲连接数
            jPoolConfig.setMaxIdle(10);
            //设置连接池最大连接数
            jPoolConfig.setMaxTotal(100);
            //设置连接池创建连接的时间
            jPoolConfig.setMaxWaitMillis(1000);
            //表示连接池在创建连接到时候会先测试一下链接是否可用，这样可以保证连接池中的连接都可用
            jPoolConfig.setTestOnBorrow(true);
            pool = new JedisPool(jPoolConfig, "172.16.101.101", 6379);
        }
        Jedis jedis=pool.getResource();
        return jedis;
    }
}

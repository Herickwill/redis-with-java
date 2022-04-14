package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.Date;
import java.util.Random;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;
    private Random random = new Random();

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = KeyHelper.getKey(windowSizeMS + ":" + name + ":" + maxHits);
            Transaction t = jedis.multi();
            long nowTimeStamp = new Date().getTime();
            t.zadd(key, nowTimeStamp, String.valueOf(random.nextInt()));
            long older = nowTimeStamp - this.windowSizeMS;
            t.zremrangeByScore(key,0,older);
            Response<Long> requests = t.zcard(key);
            t.exec();
            if(requests.get() > maxHits) {
                throw new RateLimitExceededException();
            }
            t.close();
        }
    }
}

package com.igot.cb.pores.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.pores.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Service
@Slf4j
public class CacheService {

  @Autowired
  private JedisPool jedisPool;
  @Autowired
  private ObjectMapper objectMapper;

  @Value("${spring.redis.cacheTtl}")
  private long cacheTtl;

  public Jedis getJedis() {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis;
    }
  }

  public void putCache(String key, Object object) {
    try {
      String data = objectMapper.writeValueAsString(object);
      try (Jedis jedis = jedisPool.getResource()) {
        jedis.set(Constants.REDIS_KEY_PREFIX + key, data);
//        long cacheTtl = 60000;
        jedis.expire(Constants.REDIS_KEY_PREFIX + key, cacheTtl);
      }
    } catch (Exception e) {
      log.error("Error while putting data in Redis cache: {} ", e.getMessage());
    }
  }

  public String getCache(String key) {
    try {
      return getJedis().get(Constants.REDIS_KEY_PREFIX + key);
    } catch (Exception e) {
      return null;
    }
  }

  public void deleteCache(String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.del(Constants.REDIS_KEY_PREFIX + key);
    } catch (Exception e) {
      log.error("Error while deleting data from Redis cache: {} ", e.getMessage());
    }
  }
}

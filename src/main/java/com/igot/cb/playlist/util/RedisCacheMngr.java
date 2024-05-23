package com.igot.cb.playlist.util;

import com.igot.cb.pores.config.RedisConfig;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Component
@Slf4j
public class RedisCacheMngr {

  @Autowired
  private RedisConfig redisConfig;

  @Autowired
  private JedisPool jedisPool;

  public String getContentFromCache(String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.get(key);
    } catch (Exception e) {
      log.error(e.toString());
      return null;
    }
  }

  public List<String> hget(String key, int index, String... fields) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(index);
      return jedis.hmget(key, fields);
    } catch (Exception e) {
      log.error(e.toString());
      return null;
    }
  }

  public void hset(String key, int index, Map<String, String> fieldValues) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(index);
      jedis.hmset(key, fieldValues);

    } catch (Exception e) {
      log.error(e.toString());
    }
  }

  // Method to delete a field from a hash
  public Long hdel(String key, String field, int index) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.select(index);
      return jedis.hdel(key, field);
    } catch (Exception e) {
      log.error(e.toString());
      return null;
    }
  }

}

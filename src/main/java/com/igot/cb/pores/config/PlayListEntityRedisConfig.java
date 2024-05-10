package com.igot.cb.pores.config;

import com.igot.cb.playlist.entity.PlayListEntity;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class PlayListEntityRedisConfig {
  @Bean
  public RedisTemplate<String, PlayListEntity> redisTemplateForSidJobEntity(
      RedisConnectionFactory connectionFactory) {
    RedisTemplate<String, PlayListEntity> redisTemplate = new RedisTemplate<>();
    redisTemplate.setConnectionFactory(connectionFactory);
    redisTemplate.setKeySerializer(new StringRedisSerializer());
    return redisTemplate;
  }
}

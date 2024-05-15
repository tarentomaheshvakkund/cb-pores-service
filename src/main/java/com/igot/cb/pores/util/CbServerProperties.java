package com.igot.cb.pores.util;

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
public class CbServerProperties {

  @Value("${default.content.properties}")
  private String defaultContentProperties;

  @Value("${content-service-host}")
  private String contentHost;

  @Value("${content-read-endpoint}")
  private String contentReadEndPoint;

  @Value("${content-read-endpoint-fields}")
  private String contentReadEndPointFields;

  @Value("${redis.insights.index}")
  private int redisInsightIndex;

  @Value("#{${playlist.redis.key.mapping}}")
  private Map<String, String> PlayListRedisKeyMapping;

}

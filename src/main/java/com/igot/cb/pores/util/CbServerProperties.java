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

  @Value("${notify.service.host}")
  private String notifyServiceHost;

  @Value("${notify.service.path.async}")
  private String notificationAsyncPath;

  @Value("${kafka.topic.demand.request}")
  private String demandRequestKafkaTopic;

  @Value("${notification.support.mail}")
  private String supportEmail;

  @Value("${demand.request.notification.template}")
  private String demandRequestTemplate;

  @Value("${sb.service.url}")
  private String sbUrl;

  @Value("${sunbird.user.search.endpoint}")
  private String userSearchEndPoint;

}

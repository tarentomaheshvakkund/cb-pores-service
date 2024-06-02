package com.igot.cb.pores.util;

import java.util.Arrays;
import java.util.List;
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

//  @Value("#{${playlist.redis.key.mapping}}")
//  private Map<String, String> PlayListRedisKeyMapping;

  @Value("${search.result.redis.ttl}")
  private long searchResultRedisTtl;

  @Value("${sb.api.key}")
  private String sbApiKey;

  @Value("${learner.service.url}")
  private String learnerServiceUrl;

  @Value("${sb.org.search.path}")
  private String orgSearchPath;

  @Value("${elastic.required.field.demand.json.path}")
  private String elasticDemandJsonPath;

  @Value("${elastic.required.field.content.json.path}")
  private String elasticContentJsonPath;

  @Value("${elastic.required.field.interest.json.path}")
  private String elasticInterestJsonPath;

  @Value("${elastic.required.field.bookmark.json.path}")
  private String elasticBookmarkJsonPath;

  @Value("${bookmark.duplicate.not.allowed.category}")
  private String bookmarkDuplicateNotAllowedCategory;

  public List<String> getBookmarkDuplicateNotAllowedCategory() {
    return Arrays.asList(bookmarkDuplicateNotAllowedCategory.split(","));
  }

  public void setBookmarkDuplicateNotAllowedCategory(String bookmarkDuplicateNotAllowedCategory) {
    this.bookmarkDuplicateNotAllowedCategory = bookmarkDuplicateNotAllowedCategory;
  }
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

  @Value("${sunbird.course.service.host}")
  private String sbUrl;

  @Value("${sunbird.user.search.endpoint}")
  private String userSearchEndPoint;

  @Value("${lms.user.read.path}")
  private String userReadEndPoint;

}

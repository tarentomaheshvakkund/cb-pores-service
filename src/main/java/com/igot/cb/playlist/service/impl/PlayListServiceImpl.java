package com.igot.cb.playlist.service.impl;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.playlist.entity.PlayListEntity;
import com.igot.cb.playlist.repository.PlayListRepository;
import com.igot.cb.playlist.service.ContentService;
import com.igot.cb.playlist.service.PlayListSerive;
import com.igot.cb.playlist.util.ProjectUtil;
import com.igot.cb.playlist.util.RedisCacheMngr;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.Constants;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PlayListServiceImpl implements PlayListSerive {

  @Autowired
  private PlayListRepository playListRepository;

  @Value("${playlist.redis.key}")
  private String redisKey;

  @Value("${playlist.redis.ttl}")
  private long playListRedisTtl;

  @Autowired
  private RedisTemplate<String, PlayListEntity> playListEntityRedisTemplate;

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  private RedisCacheMngr redisCacheMngr;

  @Value("${redis.insights.index}")
  private int redisInsightIndex;

  @Autowired
  private ContentService contentService;

  private Logger logger = LoggerFactory.getLogger(getClass().getName());


  @Override
  public ApiResponse createPlayList(JsonNode playListDetails) {
    ApiResponse response = new ApiResponse();
    try {
      Optional<PlayListEntity> optionalJsonNodeEntity = Optional.ofNullable(
          (PlayListEntity) playListRepository.findByOrgId(
              playListDetails.get(Constants.ORG_ID).asText()));
      PlayListEntity playListEntity = optionalJsonNodeEntity.orElse(null);

      if (optionalJsonNodeEntity.isPresent()) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrMsg("Already orgId is present so update");
        response.setResponseCode(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR);
        response.getResult().put(Constants.ID, optionalJsonNodeEntity.get().getId());
        return response;
      }
      //create playlIst method
      log.info("PlayListService::createPlayList:creating playList");
      PlayListEntity jsonNodeEntity = new PlayListEntity();
      UUID playListId = UUIDs.timeBased();
      jsonNodeEntity.setId(String.valueOf(playListId));
      jsonNodeEntity.setOrgId(playListDetails.get("orgId").asText());
      jsonNodeEntity.setData(playListDetails);
      Timestamp currentTime = new Timestamp(System.currentTimeMillis());
      jsonNodeEntity.setCreatedOn(currentTime);
      jsonNodeEntity.setUpdatedOn(currentTime);
      PlayListEntity saveJsonEntity = playListRepository.save(jsonNodeEntity);
      JsonNode childrenNode = playListDetails.get("children");
      Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
      enrichContentMaps = fetchContentDetails(childrenNode);
      ObjectNode enrichedContentJson = objectMapper.createObjectNode();
      enrichedContentJson.put("children", objectMapper.valueToTree(enrichContentMaps));
      enrichedContentJson.put("id", jsonNodeEntity.getOrgId());
      persistInRedis(enrichedContentJson, jsonNodeEntity);
      response = ProjectUtil.createDefaultResponse(Constants.API_PLAYLIST_CREATE);
      response.put(Constants.RESPONSE, Constants.SUCCESS);
      response.setResponseCode(org.springframework.http.HttpStatus.OK);
      response.getResult().put(Constants.STATUS, Constants.CREATED);
      response.getResult().put(Constants.ID, playListId);
      return response;
    } catch (Exception errMsg) {
      logger.error("Failed to Create PalyList: " + playListDetails.get(Constants.ORG_ID), errMsg);
      response.getParams().setStatus(Constants.FAILED);
      response.getParams().setErrMsg(errMsg.getMessage());
      response.setResponseCode(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  private void persistInRedis(ObjectNode enrichedContentJson, PlayListEntity jsonNodeEntity)
      throws JsonProcessingException {
    Map<String, Object> map = objectMapper.convertValue(enrichedContentJson, new TypeReference<Map<String,Object>>(){});
    Map<String, String> fieldValues = new HashMap<>();
    for (Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() instanceof List) {
        List<String> list = (List<String>) entry.getValue();
        String valueAsString = String.join(",", list);
        fieldValues.put(entry.getKey(), valueAsString);
      } else {
        String valueAsString = objectMapper.writeValueAsString(entry.getValue());
        fieldValues.put(entry.getKey(), valueAsString);
      }
    }
    Map<String, String> hsetValues = new HashMap<>();
    hsetValues.put(jsonNodeEntity.getOrgId(), objectMapper.writeValueAsString(fieldValues));
    redisCacheMngr.hset(redisKey, redisInsightIndex , hsetValues);
    log.info("playlist created");
  }

  private Map<String, Map<String, Object>> fetchContentDetails(JsonNode childrenNode) {
    Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
    if (childrenNode != null && childrenNode.isArray()) {
      childrenNode.forEach(childNode -> {
        String childId = childNode.asText();
        // Call method to fetch details for each child
        Map<String, Object> contentResponse = contentService.readContentFromCache(childId, new ArrayList<>());
        if (MapUtils.isNotEmpty(contentResponse)) {
          if (Constants.LIVE.equalsIgnoreCase((String) contentResponse.get(Constants.STATUS))) {
            Map<String, Object> enrichContentMap = new HashMap<>();
            enrichContentMap.put("do_id", childId);
            enrichContentMap.put(Constants.NAME, contentResponse.get(Constants.NAME));
            enrichContentMap.put(Constants.COMPETENCIES_V5, contentResponse.get(Constants.COMPETENCIES_V5));
            enrichContentMap.put(Constants.AVG_RATING, contentResponse.get(Constants.AVG_RATING));
            enrichContentMap.put(Constants.IDENTIFIER, contentResponse.get(Constants.IDENTIFIER));
            enrichContentMap.put(Constants.DESCRIPTION, contentResponse.get(Constants.DESCRIPTION));
            enrichContentMap.put(Constants.ADDITIONAL_TAGS, contentResponse.get(Constants.ADDITIONAL_TAGS));
            enrichContentMap.put(Constants.CONTENT_TYPE_KEY, contentResponse.get(Constants.CONTENT_TYPE_KEY));
            enrichContentMap.put(Constants.PRIMARY_CATEGORY, contentResponse.get(Constants.PRIMARY_CATEGORY));
            enrichContentMap.put(Constants.DURATION, contentResponse.get(Constants.DURATION));
            enrichContentMap.put(Constants.COURSE_APP_ICON, contentResponse.get(Constants.COURSE_APP_ICON));
            enrichContentMap.put(Constants.POSTER_IMAGE, contentResponse.get(Constants.POSTER_IMAGE));
            enrichContentMap.put(Constants.ORGANISATION, contentResponse.get(Constants.ORGANISATION));
            enrichContentMap.put(Constants.CREATOR_LOGO, contentResponse.get(Constants.CREATOR_LOGO));
            enrichContentMaps.put(childId, enrichContentMap);
          }
        }
      });
    }
    return enrichContentMaps;
  }

  @Override
  public ApiResponse readPlayList(String orgId) throws JsonProcessingException {
    log.info("PlayListService::readPlayList: reading the playList");
    ApiResponse response = new ApiResponse();
    response = ProjectUtil.createDefaultResponse(Constants.API_PLAYLIST_READ);
    if (StringUtils.isEmpty(orgId)) {
      response.getParams().setStatus(Constants.FAILED);
      response.getParams().setErrMsg("Not found");
      response.setResponseCode(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
    String playListStringFromRedis =
        redisCacheMngr.hget(redisKey, redisInsightIndex, orgId).toString();
    log.info("Cached PlayList: "+playListStringFromRedis);

    if (playListStringFromRedis == null) {
      // Fetch from postgres and add fetched playlist into redis
      Optional<PlayListEntity> optionalJsonNodeEntity = playListRepository.findById(orgId);
      PlayListEntity playListEntity = optionalJsonNodeEntity.orElse(null);
      log.info("PlayListService::readPlayList::fetched playList from postgres");
      playListEntity.getData().get(Constants.CHILDREN);
      Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
      enrichContentMaps = fetchContentDetails(playListEntity.getData().get(Constants.CHILDREN));
      ObjectNode enrichedContentJson = objectMapper.createObjectNode();
      enrichedContentJson.put("children", objectMapper.valueToTree(enrichContentMaps));
      enrichedContentJson.put("id", playListEntity.getOrgId());
      persistInRedis(enrichedContentJson, playListEntity);

    }
    JsonNode rootNode = objectMapper.readTree(playListStringFromRedis);
    JsonNode firstElement = rootNode.get(0);
    String childrenJsonString = firstElement.get("children").asText();
    JsonNode childrenNode = objectMapper.readTree(childrenJsonString);
    Map<String, JsonNode> childMap = new HashMap<>();
    List<JsonNode> dataList = new ArrayList<>();
    childrenNode.fields().forEachRemaining(entry -> {
      String childId = entry.getKey();
      JsonNode childData = entry.getValue();
      dataList.add(childData);
    });

      response.setResponseCode(org.springframework.http.HttpStatus.OK);
      response.getResult().put(Constants.CONTENT, dataList);
      return response;
  }

  @Override
  public ApiResponse updatePlayList(JsonNode playListDetails) throws JsonProcessingException {
    ApiResponse response = new ApiResponse();
    try {
      log.info("PlayListService::updatePlayList");
      Optional<PlayListEntity> optionalJsonNodeEntity = Optional.ofNullable(
          (PlayListEntity) playListRepository.findByOrgId(
              playListDetails.get(Constants.ORG_ID).asText()));
      PlayListEntity playListEntity = optionalJsonNodeEntity.orElse(null);

      if (optionalJsonNodeEntity.isPresent()){
        JsonNode fetchedData = playListEntity.getData();
        log.info("PlayListService::readPlayList::fetched playList from postgres");
        ArrayNode fetchedChildren = (ArrayNode) playListEntity.getData().get(Constants.CHILDREN);
        ArrayNode updateChildren = (ArrayNode) playListDetails.get(Constants.CHILDREN);
        ArrayNode mergedChildren = objectMapper.createArrayNode();
        mergedChildren.addAll(fetchedChildren);
        mergedChildren.addAll(updateChildren);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        playListEntity.setUpdatedOn(currentTime);
        PlayListEntity saveJsonEntity = playListRepository.save(playListEntity);
        Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
        enrichContentMaps = fetchContentDetails(mergedChildren);
        ObjectNode enrichedContentJson = objectMapper.createObjectNode();
        enrichedContentJson.put("children", objectMapper.valueToTree(enrichContentMaps));
        enrichedContentJson.put("id", playListEntity.getOrgId());
        persistInRedis(enrichedContentJson, playListEntity);
        response = ProjectUtil.createDefaultResponse(Constants.API_PLAYLIST_CREATE);
        response.put(Constants.RESPONSE, Constants.SUCCESS);
        response.setResponseCode(org.springframework.http.HttpStatus.OK);
        response.getResult().put(Constants.STATUS, Constants.CREATED);
        response.getResult().put(Constants.ID, optionalJsonNodeEntity.get().getId());
        return response;
      }
    } catch (Exception e) {
      logger.error("Failed to Create PalyList: " + playListDetails.get(Constants.ORG_ID), e);
      response.getParams().setStatus(Constants.FAILED);
      response.getParams().setErrMsg(e.getMessage());
      response.setResponseCode(org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }

    return response;
  }
}

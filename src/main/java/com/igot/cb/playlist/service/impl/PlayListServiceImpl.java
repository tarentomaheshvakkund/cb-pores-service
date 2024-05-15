package com.igot.cb.playlist.service.impl;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.playlist.dto.SearchDto;
import com.igot.cb.playlist.entity.PlayListEntity;
import com.igot.cb.playlist.repository.PlayListRepository;
import com.igot.cb.playlist.service.ContentService;
import com.igot.cb.playlist.service.PlayListSerive;
import com.igot.cb.playlist.util.ProjectUtil;
import com.igot.cb.playlist.util.RedisCacheMngr;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.CbServerProperties;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PlayListServiceImpl implements PlayListSerive {

  @Autowired
  private PlayListRepository playListRepository;

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

  @Autowired
  private CbServerProperties cbServerProperties;

  private Logger logger = LoggerFactory.getLogger(getClass().getName());


  @Override
  public ApiResponse createPlayList(JsonNode playListDetails) {
    log.info("PlayListService::createPlayList:inside the method");
    ApiResponse response = new ApiResponse();
    try {
      Optional<PlayListEntity> optionalJsonNodeEntity = Optional.ofNullable(
          (PlayListEntity) playListRepository.findByOrgId(
              playListDetails.get(Constants.ORG_ID).asText()));
      PlayListEntity playListEntity = optionalJsonNodeEntity.orElse(null);

      if (optionalJsonNodeEntity.isPresent()) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrMsg("Already orgId is present so update");
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        response.getResult().put(Constants.ID, optionalJsonNodeEntity.get().getId());
        return response;
      }
      //create playlIst method
      log.info("PlayListService::createPlayList:creating playList");
      PlayListEntity jsonNodeEntity = new PlayListEntity();
      UUID playListId = UUIDs.timeBased();
      jsonNodeEntity.setId(String.valueOf(playListId));
      jsonNodeEntity.setOrgId(playListDetails.get(Constants.ORG_ID).asText());
      jsonNodeEntity.setData(playListDetails);
      Timestamp currentTime = new Timestamp(System.currentTimeMillis());
      jsonNodeEntity.setCreatedOn(currentTime);
      jsonNodeEntity.setUpdatedOn(currentTime);
      jsonNodeEntity.setRequestType(playListDetails.get(Constants.RQST_CONTENT_TYPE).asText());
      jsonNodeEntity.setIsActive(true);
      playListRepository.save(jsonNodeEntity);
      JsonNode childrenNode = playListDetails.get(Constants.CHILDREN);
      Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
      enrichContentMaps = fetchContentDetails(childrenNode);
      ObjectNode enrichedContentJson = objectMapper.createObjectNode();
      enrichedContentJson.put(Constants.CHILDREN, objectMapper.valueToTree(enrichContentMaps));
      enrichedContentJson.put(Constants.ID, jsonNodeEntity.getOrgId());
      persistInRedis(enrichedContentJson, jsonNodeEntity);
      response = ProjectUtil.createDefaultResponse(Constants.API_PLAYLIST_CREATE);
      response.put(Constants.RESPONSE, Constants.SUCCESS);
      response.setResponseCode(HttpStatus.OK);
      response.getResult().put(Constants.STATUS, Constants.CREATED);
      response.getResult().put(Constants.ID, playListId);
      log.info("PlayListService::createPlayList:created playList");
      return response;
    } catch (Exception errMsg) {
      logger.error("Failed to Create PalyList: " + playListDetails.get(Constants.ORG_ID), errMsg);
      response.getParams().setStatus(Constants.FAILED);
      response.getParams().setErrMsg(errMsg.getMessage());
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }

  private void persistInRedis(ObjectNode enrichedContentJson, PlayListEntity jsonNodeEntity) {
    log.info("PlayListService::persistInRedis");
    try {
      Map<String, Object> map = objectMapper.convertValue(enrichedContentJson,
          new TypeReference<Map<String, Object>>() {
          });
      Map<String, String> fieldValues = new HashMap<>();
      for (Entry<String, Object> entry : map.entrySet()) {
        if (entry.getValue() instanceof List) {
          List<String> list = (List<String>) entry.getValue();
          String valueAsString = String.join(",", list);
          fieldValues.put(entry.getKey(), valueAsString);
        } else {
          String valueAsString = null;
          valueAsString = objectMapper.writeValueAsString(entry.getValue());
          fieldValues.put(entry.getKey(), valueAsString);
        }
      }
      Map<String, String> hsetValues = new HashMap<>();
      hsetValues.put(jsonNodeEntity.getOrgId(), objectMapper.writeValueAsString(fieldValues));
      redisCacheMngr.hset(
          cbServerProperties.getPlayListRedisKeyMapping().get(jsonNodeEntity.getRequestType()),
          redisInsightIndex, hsetValues);
      log.info("persisted in redis");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private Map<String, Map<String, Object>> fetchContentDetails(JsonNode childrenNode) {
    log.info("PlayListService::fetchContentDetails");
    Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
    if (childrenNode != null && childrenNode.isArray()) {
      childrenNode.forEach(childNode -> {
        String childId = childNode.asText();
        // Call method to fetch details for each child
        Map<String, Object> contentResponse = contentService.readContentFromCache(childId,
            new ArrayList<>());
        if (MapUtils.isNotEmpty(contentResponse)) {
          if (Constants.LIVE.equalsIgnoreCase((String) contentResponse.get(Constants.STATUS))) {
            Map<String, Object> enrichContentMap = new HashMap<>();
            enrichContentMap.put("do_id", childId);
            enrichContentMap.put(Constants.NAME, contentResponse.get(Constants.NAME));
            enrichContentMap.put(Constants.COMPETENCIES_V5,
                contentResponse.get(Constants.COMPETENCIES_V5));
            enrichContentMap.put(Constants.AVG_RATING, contentResponse.get(Constants.AVG_RATING));
            enrichContentMap.put(Constants.IDENTIFIER, contentResponse.get(Constants.IDENTIFIER));
            enrichContentMap.put(Constants.DESCRIPTION, contentResponse.get(Constants.DESCRIPTION));
            enrichContentMap.put(Constants.ADDITIONAL_TAGS,
                contentResponse.get(Constants.ADDITIONAL_TAGS));
            enrichContentMap.put(Constants.CONTENT_TYPE_KEY,
                contentResponse.get(Constants.CONTENT_TYPE_KEY));
            enrichContentMap.put(Constants.PRIMARY_CATEGORY,
                contentResponse.get(Constants.PRIMARY_CATEGORY));
            enrichContentMap.put(Constants.DURATION, contentResponse.get(Constants.DURATION));
            enrichContentMap.put(Constants.COURSE_APP_ICON,
                contentResponse.get(Constants.COURSE_APP_ICON));
            enrichContentMap.put(Constants.POSTER_IMAGE,
                contentResponse.get(Constants.POSTER_IMAGE));
            enrichContentMap.put(Constants.ORGANISATION,
                contentResponse.get(Constants.ORGANISATION));
            enrichContentMap.put(Constants.CREATOR_LOGO,
                contentResponse.get(Constants.CREATOR_LOGO));
            enrichContentMaps.put(childId, enrichContentMap);
          }
        }
      });
    }
    log.info("PlayListService::fetchContentDetails:fetchedContent");
    return enrichContentMaps;
  }

  @Override
  public ApiResponse searchPlayListForOrg(SearchDto searchDto) {
    log.info("PlayListService::readPlayList: reading the playList");
    ApiResponse response = new ApiResponse();
    try {
      validatePayload(searchDto);
      log.info("PlayListService::validatePayload:validate");
    } catch (Exception e) {
      response.getParams().setStatus(Constants.FAILED);
      response.setResponseCode(HttpStatus.BAD_REQUEST);
      return response;
    }
    HashMap<String, Object> request;
    request = searchDto.getRequest();
    // Get the "filters" HashMap
    HashMap<String, Object> filters = (HashMap<String, Object>) request.get(Constants.FILTERS);
    String orgId = (String) filters.get(Constants.ORGANISATION);
    List<String> contextTypes = (List<String>) filters.get(Constants.REQUEST_TYPE);
    String playListStringFromRedis = "";
    try {
      for (String contextType : contextTypes) {
        playListStringFromRedis =
            redisCacheMngr.hget(cbServerProperties.getPlayListRedisKeyMapping().get(contextType),
                redisInsightIndex, orgId).toString();
        log.info("Cached PlayList: " + playListStringFromRedis);

      }
      if (playListStringFromRedis == null || "[null]".equals(playListStringFromRedis)
          || playListStringFromRedis.isEmpty()) {
        // Fetch from postgres and add fetched playlist into redis
        Optional<PlayListEntity> optionalJsonNodeEntity = Optional.ofNullable(
            playListRepository.findByOrgId(orgId));
        PlayListEntity playListEntity = optionalJsonNodeEntity.orElse(null);
        log.info("PlayListService::readPlayList::fetched playList from postgres");
        playListEntity.getData().get(Constants.CHILDREN);
        Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
        enrichContentMaps = fetchContentDetails(playListEntity.getData().get(Constants.CHILDREN));
        ObjectNode enrichedContentJson = objectMapper.createObjectNode();
        enrichedContentJson.put(Constants.CHILDREN, objectMapper.valueToTree(enrichContentMaps));
        enrichedContentJson.put(Constants.ID, playListEntity.getOrgId());
        persistInRedis(enrichedContentJson, playListEntity);
        for (String contextType : contextTypes) {
          playListStringFromRedis =
              redisCacheMngr.hget(contextType, redisInsightIndex, orgId).toString();
          log.info("Cached PlayList: " + playListStringFromRedis);

        }
      }

      JsonNode rootNode = null;
      rootNode = objectMapper.readTree(playListStringFromRedis);
      JsonNode firstElement = rootNode.get(0);
      String childrenJsonString = firstElement.get(Constants.CHILDREN).asText();
      JsonNode childrenNode = objectMapper.readTree(childrenJsonString);
      Map<String, JsonNode> childMap = new HashMap<>();
      List<JsonNode> dataList = new ArrayList<>();
      childrenNode.fields().forEachRemaining(entry -> {
        String childId = entry.getKey();
        JsonNode childData = entry.getValue();
        dataList.add(childData);
      });

      response.setResponseCode(HttpStatus.OK);
      response.getResult().put(Constants.CONTENT, dataList);
      return response;
    } catch (Exception e) {
      logger.error("Failed to Create PalyList: ", e);
      response.getParams().setStatus(Constants.FAILED);
      response.getParams().setErrMsg(e.getMessage());
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
  }


  @Override
  public ApiResponse updatePlayList(JsonNode playListDetails) {
    ApiResponse response = new ApiResponse();
    try {
      log.info("PlayListService::updatePlayList");
      Optional<PlayListEntity> optionalJsonNodeEntity = Optional.ofNullable(
          (PlayListEntity) playListRepository.findByOrgId(
              playListDetails.get(Constants.ORG_ID).asText()));
      PlayListEntity playListEntity = optionalJsonNodeEntity.orElse(null);

      if (optionalJsonNodeEntity.isPresent()) {
        JsonNode fetchedData = playListEntity.getData();
        log.info("PlayListService::readPlayList::fetched playList from postgres");
        ArrayNode fetchedChildren = (ArrayNode) playListEntity.getData().get(Constants.CHILDREN);
        ArrayNode updateChildren = (ArrayNode) playListDetails.get(Constants.CHILDREN);
        ArrayNode mergedChildren = objectMapper.createArrayNode();
        mergedChildren.addAll(fetchedChildren);
        mergedChildren.addAll(updateChildren);
        ((ObjectNode) fetchedData).put(Constants.CHILDREN, mergedChildren);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        playListEntity.setUpdatedOn(currentTime);
        PlayListEntity saveJsonEntity = playListRepository.save(playListEntity);
        Map<String, Map<String, Object>> enrichContentMaps = new HashMap<>();
        enrichContentMaps = fetchContentDetails(mergedChildren);
        ObjectNode enrichedContentJson = objectMapper.createObjectNode();
        enrichedContentJson.put(Constants.CHILDREN, objectMapper.valueToTree(enrichContentMaps));
        enrichedContentJson.put(Constants.ID, playListEntity.getOrgId());
        persistInRedis(enrichedContentJson, playListEntity);
        response = ProjectUtil.createDefaultResponse(Constants.API_PLAYLIST_CREATE);
        response.put(Constants.RESPONSE, Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.getResult().put(Constants.STATUS, Constants.CREATED);
        response.getResult().put(Constants.ID, optionalJsonNodeEntity.get().getId());
        return response;
      }
    } catch (Exception e) {
      logger.error("Failed to Create PalyList: " + playListDetails.get(Constants.ORG_ID), e);
      response.getParams().setStatus(Constants.FAILED);
      response.getParams().setErrMsg(e.getMessage());
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
    log.info("PlayListService::updatePlayList:updated");
    return response;
  }

  @Override
  public ApiResponse delete(String id) {
    ApiResponse response = new ApiResponse();
    try {
      log.info("PlayListService::delete");
      Optional<PlayListEntity> optionalJsonNodeEntity = Optional.ofNullable(
          (PlayListEntity) playListRepository.findByOrgId(
              id));
      PlayListEntity playListEntity = optionalJsonNodeEntity.orElse(null);

      if (optionalJsonNodeEntity.isPresent()) {
        log.info("PlayListService::delete::deleting");
        playListEntity.setIsActive(false);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        playListEntity.setUpdatedOn(currentTime);
        playListRepository.save(playListEntity);
        log.info("PlayListService::delete::deleted");
        response = ProjectUtil.createDefaultResponse(Constants.API_PLAYLIST_CREATE);
        response.put(Constants.RESPONSE, Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
        response.getResult().put(Constants.STATUS, Constants.DELETED_SUCCESSFULLY);
        response.getResult().put(Constants.ID, optionalJsonNodeEntity.get().getId());
        return response;
      }
    } catch (Exception e) {
      logger.error("Failed to delete PalyList: " + id, e);
      response.getParams().setStatus(Constants.FAILED);
      response.getParams().setErrMsg(e.getMessage());
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
      return response;
    }
    return response;
  }

  private void validatePayload(SearchDto searchDto) {
    log.info("PlayListService::validatePayload:inside method");
    if (searchDto == null || searchDto.getRequest() == null) {
      throw new IllegalArgumentException("Request structure is missing or invalid");
    }
    Map<String, Object> request = searchDto.getRequest();
    if (!request.containsKey(Constants.FILTERS) || !(request.get(
        Constants.FILTERS) instanceof Map)) {
      throw new IllegalArgumentException("filters field is missing or invalid");
    }
    Map<String, Object> filters = (HashMap<String, Object>) request.get(Constants.FILTERS);
    if (!filters.containsKey(Constants.REQUEST_TYPE) || !(filters.get(
        Constants.REQUEST_TYPE) instanceof List) || ((List<?>) filters.get(
        Constants.REQUEST_TYPE)).isEmpty()) {
      throw new IllegalArgumentException("contextType field is missing or empty");
    }
    if (!filters.containsKey(Constants.ORGANISATION) || !(filters.get(
        Constants.ORGANISATION) instanceof String) || ((String) filters.get(
        Constants.ORGANISATION)).isEmpty()) {
      throw new IllegalArgumentException("organisation field is missing or empty");
    }
  }

}

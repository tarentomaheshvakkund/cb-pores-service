package com.igot.cb.playlist.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.demand.entity.DemandEntity;
import com.igot.cb.playlist.entity.PlayListEntity;
import com.igot.cb.playlist.repository.PlayListRepository;
import com.igot.cb.playlist.service.PlayListSerive;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
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


  @Override
  public CustomResponse createPlayList(JsonNode playListDetails) {
    CustomResponse response = new CustomResponse();
    try {
      log.info("PlayListService::createPlayList:creating playList");
      String playListId = String.valueOf(UUID.randomUUID());
      ((ObjectNode) playListDetails).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
      Timestamp currentTime = new Timestamp(System.currentTimeMillis());
      ((ObjectNode) playListDetails).put(Constants.CREATED_DATE, String.valueOf(currentTime));
      ((ObjectNode) playListDetails).put(Constants.LAST_UPDATED_DATE, String.valueOf(currentTime));
      ((ObjectNode) playListDetails).put("playListId", playListId);
      PlayListEntity jsonNodeEntity = new PlayListEntity();
      jsonNodeEntity.setPlayListId(playListId);
      jsonNodeEntity.setOrgId(playListDetails.get("orgId").asText());
      jsonNodeEntity.setData(playListDetails);
      jsonNodeEntity.setCreatedOn(currentTime);
      jsonNodeEntity.setUpdatedOn(currentTime);
      PlayListEntity saveJsonEntity = playListRepository.save(jsonNodeEntity);
      playListEntityRedisTemplate.opsForValue()
          .set(redisKey + jsonNodeEntity.getPlayListId(), jsonNodeEntity, playListRedisTtl,
              TimeUnit.SECONDS);
      log.info("playlist created");
      response.setMessage("Successfully created");
      response.setResponseCode(org.springframework.http.HttpStatus.valueOf(HttpStatus.SC_OK));
      response
          .getResult()
          .put(Constants.RESULT,
              objectMapper.convertValue(
                  jsonNodeEntity.getData(), new TypeReference<Object>() {
                  }));
      return response;
    } catch (Exception e) {
      throw new CustomException("ERROR02", "Error while creating the playList");
    }
  }

  @Override
  public CustomResponse readPlayList(String playListId) {
    log.info("PlayListService::readPlayList: reading the playList");
    CustomResponse response = new CustomResponse();
    if (StringUtils.isEmpty(playListId)) {
      response.setResponseCode(org.springframework.http.HttpStatus.valueOf(HttpStatus.SC_INTERNAL_SERVER_ERROR));
      response.setMessage("Id not found");
      return response;
    }
    PlayListEntity playListEntity =
        (PlayListEntity) playListEntityRedisTemplate.opsForValue().get(redisKey + playListId);
    if (playListEntity != null) {
      log.info("PlayListService::readPlayList::fetched playList from redis");
      response
          .getResult()
          .put(Constants.RESULT,
              objectMapper.convertValue(
                  playListEntity.getData(), new TypeReference<Object>() {
                  }));
      return response;
    }
    if (playListEntity == null) {
      // Fetch from postgres and add fetched playlist into redis
      Optional<PlayListEntity> optionalJsonNodeEntity = playListRepository.findById(playListId);
      playListEntity = optionalJsonNodeEntity.orElse(null);
      log.info("PlayListService::readPlayList::fetched playList from postgres");
      playListEntityRedisTemplate.opsForValue()
          .set(redisKey + playListId, playListEntity, playListRedisTtl,
              TimeUnit.SECONDS);
      response
          .getResult()
          .put(Constants.RESULT,
              objectMapper.convertValue(
                  playListEntity.getData(), new TypeReference<Object>() {
                  }));
    }
    return response;
  }
}

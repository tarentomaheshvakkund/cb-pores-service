package com.igot.cb.announcement.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.igot.cb.announcement.entity.AnnouncementEntity;
import com.igot.cb.announcement.repository.AnnouncementRepository;
import com.igot.cb.announcement.service.AnnouncementService;
import com.igot.cb.contentprovider.service.impl.ContentPartnerServiceImpl;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PayloadValidation;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AnnouncementServiceImpl implements AnnouncementService {

  @Autowired
  private PayloadValidation payloadValidation;

  private Logger logger = LoggerFactory.getLogger(ContentPartnerServiceImpl.class);
  
  @Autowired
  private AnnouncementRepository announcementRepository;

  @Autowired
  private EsUtilService esUtilService;

  @Autowired
  private CacheService cacheService;
  @Autowired
  private ObjectMapper objectMapper;

  private String requiredJsonFilePath = "/EsFieldsmapping/announcementEsMapping.json";


  @Override
  public CustomResponse createAnnouncement(JsonNode announcementEntity) {
    log.info("AnnouncementServiceImpl::createAnnouncement:inside");
    CustomResponse response = new CustomResponse();
    payloadValidation.validatePayload(Constants.ANNOUNCEMENT_VALIDATION_FILE_JSON, announcementEntity);
    try {
      if (announcementEntity.get(Constants.ID) == null) {
        log.info("AnnouncementServiceImpl::createAnnouncement:creating announcement");
        String id = String.valueOf(UUID.randomUUID());
        ((ObjectNode) announcementEntity).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        AnnouncementEntity jsonNodeEntity = new AnnouncementEntity();
        jsonNodeEntity.setAnnouncementId(id);
        jsonNodeEntity.setData(announcementEntity);
        jsonNodeEntity.setCreatedOn(currentTime);
        jsonNodeEntity.setUpdatedOn(currentTime);
        jsonNodeEntity.setIsActive(true);
        AnnouncementEntity saveJsonEntity = announcementRepository.save(jsonNodeEntity);
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.set(Constants.ANNOUNCEMENT_ID, new TextNode(saveJsonEntity.getAnnouncementId()));
        jsonNode.setAll((ObjectNode) saveJsonEntity.getData());
        Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
        esUtilService.addDocument(Constants.ANNOUNCEMENT_INDEX, Constants.INDEX_TYPE, id, map, requiredJsonFilePath);
        cacheService.putCache(jsonNodeEntity.getAnnouncementId(), jsonNode);
        map.put(Constants.ANNOUNCEMENT_ID, id);
        response.setResult(map);
        log.info("announcement created");
        response.setMessage(Constants.SUCCESSFULLY_CREATED);
      } else {
        log.info("Updating announcement");
        String exitingId = announcementEntity.get(Constants.ANNOUNCEMENT_ID).asText();
        Optional<AnnouncementEntity> content = announcementRepository.findById(exitingId);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        if (content.isPresent()) {
          AnnouncementEntity josnEntity = content.get();
          josnEntity.setData(announcementEntity);
          josnEntity.setUpdatedOn(currentTime);
          AnnouncementEntity updateJsonEntity = announcementRepository.save(josnEntity);
          if (!org.springframework.util.ObjectUtils.isEmpty(updateJsonEntity)) {
            Map<String, Object> jsonMap =
                objectMapper.convertValue(updateJsonEntity.getData(), new TypeReference<Map<String, Object>>() {
                });
            updateJsonEntity.setAnnouncementId(exitingId);
            esUtilService.updateDocument(Constants.INDEX_NAME, Constants.INDEX_TYPE, exitingId, jsonMap, requiredJsonFilePath);
            cacheService.putCache(exitingId, updateJsonEntity);
            log.info("updated the announcement");
            jsonMap.put(Constants.ANNOUNCEMENT_ID, exitingId);
            response.setResult(jsonMap);
            response.setMessage(Constants.SUCCESSFULLY_UPDATED);
          }
        }
      }
      response.setResponseCode(HttpStatus.OK);
      return response;
    } catch (Exception e) {
      logger.error("error while processing", e);
      throw new RuntimeException(e);
    }
  }
}

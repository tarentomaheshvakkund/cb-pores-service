package com.igot.cb.contentpartner.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.contentpartner.entity.ContentPartnerEntity;
import com.igot.cb.contentpartner.repository.ContentPartnerRepository;
import com.igot.cb.contentpartner.service.ContentPartnerService;
import com.igot.cb.playlist.util.ProjectUtil;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PayloadValidation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.sql.Timestamp;
import java.util.*;

@Service
@Slf4j
public class ContentPartnerServiceImpl implements ContentPartnerService {

    @Autowired
    private EsUtilService esUtilService;

    @Autowired
    private ContentPartnerRepository entityRepository;
    @Autowired
    private CacheService cacheService;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private PayloadValidation payloadValidation;

    private Logger logger = LoggerFactory.getLogger(ContentPartnerServiceImpl.class);

    @Override
    public ApiResponse createOrUpdate(JsonNode partnerDetails) {
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_PARTNER_CREATE);
        payloadValidation.validatePayload(Constants.PAYLOAD_VALIDATION_FILE_CONTENT_PROVIDER, partnerDetails);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        try {
            if (partnerDetails.get(Constants.ID) == null) {
                Optional<ContentPartnerEntity> optionalEntity = entityRepository.findByContentPartnerName(partnerDetails.get("contentPartnerName").asText());
                if (optionalEntity.isPresent()) {
                    response.getParams().setErrMsg("Content partner name already present in DB");
                    response.getParams().setStatus(Constants.FAILED);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return response;
                }
                if (partnerDetails.path("partnerCode").asText() != null && !partnerDetails.path("partnerCode").asText().isEmpty()) {
                    if (entityRepository.findByPartnerCode(partnerDetails.get("partnerCode").asText()).isPresent()) {
                        response.getParams().setErrMsg("Content partner code already present in DB");
                        response.getParams().setStatus(Constants.FAILED);
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                        return response;
                    }
                }
                log.info("ContentPartnerServiceImpl::createOrUpdate:creating content partner provider");
                String id = String.valueOf(UUID.randomUUID());
                ((ObjectNode) partnerDetails).put(Constants.PARTNERCODE, partnerDetails.path("partnerCode").asText(null));
                ((ObjectNode) partnerDetails).put(Constants.ID, id);
                ((ObjectNode) partnerDetails).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
                ((ObjectNode) partnerDetails).put(Constants.CREATED_ON, String.valueOf(currentTime));
                ((ObjectNode) partnerDetails).put(Constants.UPDATED_ON, String.valueOf(currentTime));
                if(partnerDetails.path(Constants.IS_AUTHENTICATE).isMissingNode()){
                    ((ObjectNode) partnerDetails).put(Constants.IS_AUTHENTICATE, Constants.ACTIVE_STATUS_AUTHENTICATE);
                }
                ContentPartnerEntity contentPartnerEntity = new ContentPartnerEntity();
                contentPartnerEntity.setId(id);
                contentPartnerEntity.setCreatedOn(currentTime);
                contentPartnerEntity.setUpdatedOn(currentTime);
                contentPartnerEntity.setIsActive(Constants.ACTIVE_STATUS);
                contentPartnerEntity.setTrasformContentJson(partnerDetails.get("trasformContentJson"));
                contentPartnerEntity.setTransformProgressJson(partnerDetails.get("transformProgressJson"));
                contentPartnerEntity.setTrasformCertificateJson(partnerDetails.get("trasformCertificateJson"));
                ObjectNode objectNode = (ObjectNode) partnerDetails;
                objectNode.remove("trasformContentJson");
                objectNode.remove("transformProgressJson");
                objectNode.remove("trasformCertificateJson");
                addSearchTags(objectNode);
                contentPartnerEntity.setData(objectNode);
                ContentPartnerEntity saveJsonEntity = entityRepository.save(contentPartnerEntity);
                Map<String, Object> map = objectMapper.convertValue(saveJsonEntity.getData(), Map.class);
                esUtilService.addDocument(Constants.CONTENT_PROVIDER_INDEX_NAME, Constants.INDEX_TYPE, id, map, cbServerProperties.getElasticContentJsonPath());
                Map<String,Object> result=objectMapper.convertValue(saveJsonEntity, Map.class);
                cacheService.putCache(saveJsonEntity.getId(), result);
                log.info("Content partner created");
                response.setResult(result);
                response.setResponseCode(HttpStatus.OK);
            } else {
                log.info("Updating content partner entity");
                response = ProjectUtil.createDefaultResponse(Constants.API_PARTNER_UPDATE);
                String existingId = partnerDetails.get("id").asText();
                Optional<ContentPartnerEntity> content = entityRepository.findById(existingId);
                if (content.isPresent()) {
                    if (partnerDetails.path("partnerCode").asText() != null&&!partnerDetails.path("partnerCode").asText().isEmpty()) {
                        Optional<ContentPartnerEntity> existingPartnerCodeEntity = entityRepository.findByPartnerCode(partnerDetails.get("partnerCode").asText());
                        if (existingPartnerCodeEntity.isPresent() && !existingPartnerCodeEntity.get().getId().equals(existingId)) {
                            response.getParams().setErrMsg("Content partner code already present in DB");
                            response.getParams().setStatus(Constants.FAILED);
                            response.setResponseCode(HttpStatus.BAD_REQUEST);
                            return response;
                        }
                    }

                    if (entityRepository.findByContentPartnerName(partnerDetails.get("contentPartnerName").asText()).filter(entity -> !entity.getId().equals(existingId)).isPresent()) {
                        response.getParams().setErrMsg("Content partner name already present in DB");
                        response.getParams().setStatus(Constants.FAILED);
                        response.setResponseCode(HttpStatus.BAD_REQUEST);
                        return response;
                    }
                    ((ObjectNode) partnerDetails).put(Constants.PARTNERCODE, partnerDetails.path("partnerCode").asText(null));
                    ((ObjectNode) partnerDetails).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
                    ((ObjectNode) partnerDetails).put(Constants.CREATED_ON, String.valueOf(content.get().getCreatedOn()));
                    ((ObjectNode) partnerDetails).put(Constants.UPDATED_ON, String.valueOf(currentTime));
                    if(partnerDetails.path(Constants.IS_AUTHENTICATE).isMissingNode()){
                        ((ObjectNode) partnerDetails).put(Constants.IS_AUTHENTICATE, content.get().getData().get(Constants.IS_AUTHENTICATE));
                    }
                    ContentPartnerEntity jsonEntity = content.get();
                    jsonEntity.setUpdatedOn(currentTime);
                    jsonEntity.setIsActive(Constants.ACTIVE_STATUS);
                    jsonEntity.setTrasformContentJson(partnerDetails.get("trasformContentJson"));
                    jsonEntity.setTransformProgressJson(partnerDetails.get("transformProgressJson"));
                    jsonEntity.setTrasformCertificateJson(partnerDetails.get("trasformCertificateJson"));
                    ObjectNode objectNode= (ObjectNode) partnerDetails;
                    objectNode.remove("trasformContentJson");
                    objectNode.remove("transformProgressJson");
                    objectNode.remove("trasformCertificateJson");
                    addSearchTags(objectNode);
                    jsonEntity.setData(objectNode);
                    ContentPartnerEntity updateJsonEntity = entityRepository.save(jsonEntity);
                    if (!ObjectUtils.isEmpty(updateJsonEntity)) {
                        Map<String, Object> jsonMap =
                                objectMapper.convertValue(updateJsonEntity.getData(), new TypeReference<Map<String, Object>>() {
                                });
                        esUtilService.updateDocument(Constants.CONTENT_PROVIDER_INDEX_NAME, Constants.INDEX_TYPE, existingId, jsonMap, cbServerProperties.getElasticContentJsonPath());
                        Map<String,Object> result=objectMapper.convertValue(updateJsonEntity, Map.class);
                        cacheService.putCache(updateJsonEntity.getId(), result);
                        log.info("updated the content partner");
                        response.setResult(result);
                        response.setResponseCode(HttpStatus.OK);
                    }
                }else {
                    response.getParams().setErrMsg("Data not present in DB With given ID");
                    response.getParams().setStatus(Constants.FAILED);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
            }
            return response;
        } catch (Exception e) {
            response.getParams().setErrMsg(e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    private JsonNode addSearchTags(JsonNode formattedData) {
        List<String> searchTags = new ArrayList<>();
        searchTags.add(formattedData.get("contentPartnerName").textValue().toLowerCase());
        ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
        ((ObjectNode) formattedData).put("searchTags", searchTagsArray);
        return formattedData;
    }


    @Override
    public ApiResponse read(String id) {
        log.info("ContentPartnerServiceImpl::read:reading information about the content partner");
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_PARTNER_READ);
        if (StringUtils.isEmpty(id)) {
            response.getParams().setErrMsg(Constants.ID_NOT_FOUND);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        try {
            String cachedJson = cacheService.getCache(id);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info("Record coming from redis cache");
                response.setResponseCode(HttpStatus.OK);
                response.setResult(objectMapper.readValue(cachedJson, new TypeReference<Map>() {}));
            } else {
                Optional<ContentPartnerEntity> entityOptional = entityRepository.findByIdAndIsActive(id,true);
                if (entityOptional.isPresent()) {
                    ContentPartnerEntity entity = entityOptional.get();
                    cacheService.putCache(id, entity);
                    log.info("Record coming from postgres db");
                    response.setResponseCode(HttpStatus.OK);
                    response.setResult(objectMapper.convertValue(entity, Map.class));
                } else {
                    response.getParams().setErrMsg(Constants.INVALID_ID);
                    response.getParams().setStatus(Constants.FAILED);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
            }
        } catch (Exception e) {
            log.error("error while processing", e);
            response.getParams().setErrMsg(e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public ApiResponse searchEntity(SearchCriteria searchCriteria) {
        log.info("ContentPartnerServiceImpl::searchEntity:searching the content partner");
        String searchString = searchCriteria.getSearchString();
        ApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_PARTNER_SEARCH);
        if (searchString != null && searchString.length() < 2) {
            response.getParams().setErrMsg("Minimum 3 characters are required to search");
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
        }
        try {
            SearchResult searchResult =
                    esUtilService.searchDocuments(Constants.CONTENT_PROVIDER_INDEX_NAME, searchCriteria);
            Map<String, Object> jsonMap =
                    objectMapper.convertValue(searchResult, new TypeReference<Map<String, Object>>() {
                    });
            response.setResult(jsonMap);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Error while processing to search", e);
            response.getParams().setErrMsg(e.getMessage());
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public ApiResponse delete(String id) {
        log.info("ContentPartnerServiceImpl::delete:deleting the content partner");
        ApiResponse response=ProjectUtil.createDefaultResponse(Constants.API_PARTNER_DELETE);
        try {
            if (StringUtils.isNotEmpty(id)) {
                Optional<ContentPartnerEntity> entityOptional = entityRepository.findByIdAndIsActive(id,true);
                if (entityOptional.isPresent()) {
                    ContentPartnerEntity josnEntity = entityOptional.get();
                    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                    josnEntity.setUpdatedOn(currentTime);
                    josnEntity.setIsActive(Constants.ACTIVE_STATUS_FALSE);
                    ((ObjectNode) josnEntity.getData()).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS_FALSE);
                    entityRepository.save(josnEntity);
                    Map<String, Object> map = objectMapper.convertValue(josnEntity.getData(), Map.class);
                    esUtilService.addDocument(Constants.CONTENT_PROVIDER_INDEX_NAME, Constants.INDEX_TYPE, id, map, cbServerProperties.getElasticContentJsonPath());
                    cacheService.deleteCache(id);
                    Map<String,Object> map1=new HashMap<>();
                    map1.put(id,Constants.DELETED_SUCCESSFULLY);
                    response.setResponseCode(HttpStatus.OK);
                    response.setResult(map1);
                } else {
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    response.getParams().setErrMsg(Constants.CONTENT_PARTNER_NOT_FOUND);
                }
            } else {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setErrMsg(Constants.INVALID_ID);
            }
        } catch (Exception e) {
            log.error("Error deleting Entity with ID " + id + " " + e.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setErrMsg("Error deleting Entity with ID " + id + " " + e.getMessage());
        }
        return response;
    }

    public ApiResponse getContentDetailsByOrgId(String partnercode) {
        log.info("CiosContentService:: ContentPartnerEntity: getContentDetailsByPartnerName {}",partnercode);
        try {
            ApiResponse response=ProjectUtil.createDefaultResponse(Constants.API_PARTNER_READ);
            ContentPartnerEntity entity=null;
            String cachedJson = cacheService.getCache(partnercode);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info("Record coming from redis cache");
                response.setResponseCode(HttpStatus.OK);
                response.setResult(objectMapper.readValue(cachedJson, new TypeReference<Map>() {}));
            } else {
                Optional<ContentPartnerEntity> entityOptional = entityRepository.findByPartnerCode(partnercode);
                if (entityOptional.isPresent()) {
                    log.info("Record coming from postgres db");
                    entity = entityOptional.get();
                    cacheService.putCache(partnercode,entity);
                    response.setResponseCode(HttpStatus.OK);
                    response.setResult(objectMapper.convertValue(entity, Map.class));
                } else {
                    response.getParams().setErrMsg("Invalid name");
                    response.getParams().setStatus(Constants.FAILED);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                }
            }
            return response;
        } catch (Exception e) {
            log.error("error while processing", e);
        }
        return null;
    }
}

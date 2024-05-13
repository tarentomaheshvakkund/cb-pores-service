package com.igot.cb.contentprovider.servise.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.igot.cb.contentprovider.entity.ContentPartnerEntity;
import com.igot.cb.contentprovider.repository.ContentPartnerRepository;
import com.igot.cb.contentprovider.servise.ContentPartnerService;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.dto.RespParam;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.Constants;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

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

    @Override
    public CustomResponse createOrUpdate(JsonNode partnerDetails) {
        CustomResponse response = new CustomResponse();
        validatePayload(Constants.PAYLOAD_VALIDATION_FILE_CONTENT_PROVIDER, partnerDetails);
        try {
            if (partnerDetails.get("id") == null) {
                log.info("ContentPartnerServiceImpl::createOrUpdate:creating content partner provider");
                String id = String.valueOf(UUID.randomUUID());
                ((ObjectNode) partnerDetails).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
                Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                ContentPartnerEntity jsonNodeEntity = new ContentPartnerEntity();
                jsonNodeEntity.setId(id);
                jsonNodeEntity.setData(partnerDetails);
                jsonNodeEntity.setCreatedOn(currentTime);
                jsonNodeEntity.setUpdatedOn(currentTime);
                ContentPartnerEntity saveJsonEntity = entityRepository.save(jsonNodeEntity);
                ObjectMapper objectMapper = new ObjectMapper();
                ObjectNode jsonNode = objectMapper.createObjectNode();
                jsonNode.set(Constants.CONTENT_PROVIDER_ID, new TextNode(saveJsonEntity.getId()));
                jsonNode.setAll((ObjectNode) saveJsonEntity.getData());
                Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
                esUtilService.addDocument(Constants.INDEX_NAME, "_doc", id, map);
                cacheService.putCache(jsonNodeEntity.getId(), jsonNode);
                log.info("Content partner created");
                response.setMessage("Successfully created");
            } else {
                log.info("Updating content partner entity");
                String exitingId = partnerDetails.get("id").asText();
                Optional<ContentPartnerEntity> content = entityRepository.findById(exitingId);
                Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                if (content.isPresent()) {
                    ContentPartnerEntity josnEntity = content.get();
                    josnEntity.setData(partnerDetails);
                    josnEntity.setUpdatedOn(currentTime);
                    ContentPartnerEntity updateJsonEntity = entityRepository.save(josnEntity);
                    if (!org.springframework.util.ObjectUtils.isEmpty(updateJsonEntity)) {
                        Map<String, Object> jsonMap =
                                objectMapper.convertValue(updateJsonEntity.getData(), new TypeReference<Map<String, Object>>() {
                                });
                        updateJsonEntity.setId(exitingId);
                        esUtilService.updateDocument(Constants.INDEX_NAME, "_doc", exitingId, jsonMap);
                        cacheService.putCache(exitingId, updateJsonEntity);
                        log.info("updated the content partner");
                        response.setMessage("Successfully Updated");
                    }
                }
            }
            response.setResponseCode(org.springframework.http.HttpStatus.valueOf(HttpStatus.SC_OK));
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public CustomResponse read(String id) {
        log.info("ContentPartnerServiceImpl::read:reading information about the content partner");
        CustomResponse response = new CustomResponse();
        if (StringUtils.isEmpty(id)) {
            response.setResponseCode(org.springframework.http.HttpStatus.valueOf(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            response.setMessage("Id not found");
            return response;
        }
        try {
            String cachedJson = cacheService.getCache(id);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info("Record coming from redis cache");
                response
                        .getResult()
                        .put(Constants.RESULT, objectMapper.readValue(cachedJson, new TypeReference<Object>() {
                        }));
            } else {
                Optional<ContentPartnerEntity> entityOptional = entityRepository.findById(id);
                if (entityOptional.isPresent()) {
                    ContentPartnerEntity entity = entityOptional.get();
                    cacheService.putCache(id, entity.getData());
                    log.info("Record coming from postgres db");
                    response
                            .getResult()
                            .put(Constants.RESULT,
                                    objectMapper.convertValue(
                                            entity.getData(), new TypeReference<Object>() {
                                            }));
                } else {
                    response.setResponseCode(org.springframework.http.HttpStatus.BAD_REQUEST);
                }
            }
        } catch (JsonMappingException e) {
            throw new CustomException(Constants.ERROR, "error while processing", org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    @Override
    public CustomResponse searchEntity(SearchCriteria searchCriteria) {
        log.info("ContentPartnerServiceImpl::searchEntity:searching the content partner");
        String searchString = searchCriteria.getSearchString();
        CustomResponse response = new CustomResponse();
        if (searchString != null && searchString.length() < 2) {
            createErrorResponse(
                    response,
                    "Minimum 3 characters are required to search",
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    Constants.FAILED_CONST);
            return response;
        }
        try {
            SearchResult searchResult =
                    esUtilService.searchDocuments(Constants.INDEX_NAME, searchCriteria);
            response.getResult().put(Constants.RESULT, searchResult);
            createSuccessResponse(response);
            return response;
        } catch (Exception e) {
            createErrorResponse(
                    response, e.getMessage(), org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR, Constants.FAILED_CONST);
            return response;
        }
    }

    @Override
    public String delete(String id) {
        log.info("ContentPartnerServiceImpl::delete:deleting the content partner");
        try {
            if (StringUtils.isNotEmpty(id)) {
                Optional<ContentPartnerEntity> entityOptional = entityRepository.findById(id);
                if (entityOptional.isPresent()) {
                    ContentPartnerEntity josnEntity = entityOptional.get();
                    JsonNode data = josnEntity.getData();
                    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                    if (data.get(Constants.IS_ACTIVE).asBoolean()) {
                        ((ObjectNode) data).put("isActive", false);
                        josnEntity.setData(data);
                        josnEntity.setId(id);
                        josnEntity.setUpdatedOn(currentTime);
                        ContentPartnerEntity updateJsonEntity = entityRepository.save(josnEntity);
                        Map<String, Object> map = objectMapper.convertValue(data, Map.class);
                        esUtilService.addDocument(Constants.INDEX_NAME, "_doc", id, map);
                        cacheService.putCache(id, data);
                        return "content partner details deleted successfully.";
                    } else
                        return "content partner is already inactive.";
                } else return "content partner not found.";
            } else return "Invalid content partner ID.";
        } catch (Exception e) {
            return "Error deleting Entity with ID " + id + " " + e.getMessage();
        }
    }

    public void validatePayload(String fileName, JsonNode payload) {
        try {
            JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
            InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(fileName);
            JsonSchema schema = schemaFactory.getSchema(schemaStream);

            Set<ValidationMessage> validationMessages = schema.validate(payload);
            if (!validationMessages.isEmpty()) {
                StringBuilder errorMessage = new StringBuilder("Validation error(s): ");
                for (ValidationMessage message : validationMessages) {
                    errorMessage.append(message.getMessage());
                }
                throw new CustomException(Constants.ERROR, errorMessage.toString(), org.springframework.http.HttpStatus.BAD_REQUEST);
            }
        } catch (CustomException e) {
            throw new CustomException(Constants.ERROR, "Failed to validate payload: " + e.getMessage(), org.springframework.http.HttpStatus.BAD_REQUEST);
        }
    }

    public void createSuccessResponse(CustomResponse response) {
        response.setParams(new RespParam());
        response.getParams().setStatus("SUCCESS");
        response.setResponseCode(org.springframework.http.HttpStatus.OK);
    }

    public void createErrorResponse(
            CustomResponse response, String errorMessage, org.springframework.http.HttpStatus httpStatus, String status) {
        response.setParams(new RespParam());
        response.getParams().setStatus(status);
        response.setResponseCode(httpStatus);
    }
}

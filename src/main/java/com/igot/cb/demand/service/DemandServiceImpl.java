package com.igot.cb.demand.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.igot.cb.pores.config.RedisConfig;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.demand.repository.DemandRepository;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.dto.RespParam;
import com.igot.cb.demand.entity.DemandEntity;
import com.igot.cb.pores.util.Constants;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
public class DemandServiceImpl implements DemandService {

  @Autowired
  private EsUtilService esUtilService;

  @Autowired
  private DemandRepository demandRepository;
  @Autowired
  private CacheService cacheService;
  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private RedisTemplate<String, SearchResult> searchResultRedisTemplate;

  @Value("${search.result.redis.ttl}")
  private long searchResultRedisTtl;

  @Override
  public CustomResponse createDemand(JsonNode demandDetails) {
    CustomResponse response = new CustomResponse();
    validatePayload(Constants.PAYLOAD_VALIDATION_FILE, demandDetails);
    try {
      log.info("DemandService::createDemand:creating demand");
      String id = String.valueOf(UUID.randomUUID());
      ((ObjectNode) demandDetails).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
      Timestamp currentTime = new Timestamp(System.currentTimeMillis());
      ((ObjectNode) demandDetails).put(Constants.CREATED_DATE, String.valueOf(currentTime));
      ((ObjectNode) demandDetails).put(Constants.LAST_UPDATED_DATE, String.valueOf(currentTime));

      DemandEntity jsonNodeEntity = new DemandEntity();
      jsonNodeEntity.setDemandId(id);
      jsonNodeEntity.setData(demandDetails);
      jsonNodeEntity.setCreatedOn(currentTime);
      jsonNodeEntity.setUpdatedOn(currentTime);

      DemandEntity saveJsonEntity = demandRepository.save(jsonNodeEntity);

      ObjectMapper objectMapper = new ObjectMapper();
      ObjectNode jsonNode = objectMapper.createObjectNode();
      jsonNode.set(Constants.DEM_ID, new TextNode(saveJsonEntity.getDemandId()));
      jsonNode.setAll((ObjectNode) saveJsonEntity.getData());

      Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
      esUtilService.addDocument(Constants.INDEX_NAME, "_doc", id, map);

      cacheService.putCache(jsonNodeEntity.getDemandId(), jsonNode);
      log.info("demand created");
      response.setMessage("Successfully created");
      response.setResponseCode(org.springframework.http.HttpStatus.valueOf(HttpStatus.SC_OK));
      return response;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public CustomResponse readDemand(String id) {
    log.info("reading demands for content");
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
        Optional<DemandEntity> entityOptional = demandRepository.findById(id);
        if (entityOptional.isPresent()) {
          DemandEntity demandEntity = entityOptional.get();
          cacheService.putCache(id, demandEntity.getData());
          log.info("Record coming from postgres db");
          response
              .getResult()
              .put(Constants.RESULT,
                  objectMapper.convertValue(
                      demandEntity.getData(), new TypeReference<Object>() {
                      }));
        } else {
          response.setResponseCode(org.springframework.http.HttpStatus.BAD_REQUEST);
        }
      }
    } catch (JsonMappingException e) {
      throw new CustomException(Constants.ERROR, "error while processing", HttpStatus.SC_INTERNAL_SERVER_ERROR);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return response;
  }

  @Override
  public CustomResponse searchDemand(SearchCriteria searchCriteria) {
    log.info("DemandServiceImpl::searchDemand");
    CustomResponse response = new CustomResponse();
    SearchResult searchResult = new SearchResult();
    searchResult =  searchResultRedisTemplate.opsForValue().get(generateRedisJwtTokenKey(searchCriteria));
    if(searchResult != null) {
      log.info("SidJobServiceImpl::searchJobs: job search result fetched from redis");
      response.getResult().put(Constants.RESULT, searchResult);
      createSuccessResponse(response);
      return response;
    }
    String searchString = searchCriteria.getSearchString();
    if (searchString != null && searchString.length() < 2) {
      createErrorResponse(
          response,
          "Minimum 3 characters are required to search",
          org.springframework.http.HttpStatus.BAD_REQUEST,
          Constants.FAILED_CONST);
      return response;
    }
    try {
      searchResult =
          esUtilService.searchDocuments(Constants.INDEX_NAME, searchCriteria);
      response.getResult().put(Constants.RESULT, searchResult);
      createSuccessResponse(response);
      return response;
    } catch (Exception e) {
      createErrorResponse(
          response, e.getMessage(), org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR, Constants.FAILED_CONST);
      searchResultRedisTemplate.opsForValue()
          .set(generateRedisJwtTokenKey(searchCriteria), searchResult, searchResultRedisTtl,
              TimeUnit.SECONDS);
      return response;
    }
  }

  public String generateRedisJwtTokenKey(Object requestPayload) {
    if (requestPayload != null) {
      try {
        String reqJsonString = objectMapper.writeValueAsString(requestPayload);
        return JWT.create()
            .withClaim(Constants.REQUEST_PAYLOAD, reqJsonString)
            .sign(Algorithm.HMAC256(Constants.JWT_SECRET_KEY));
      } catch (JsonProcessingException e) {
        log.error("Error occurred while converting json object to json string", e);
      }
    }
    return "";
  }

  @Override
  public String delete(String id) {
    try {
      if (StringUtils.isNotEmpty(id)) {
        Optional<DemandEntity> entityOptional = demandRepository.findById(id);
        if (entityOptional.isPresent()) {
          DemandEntity josnEntity = entityOptional.get();
          JsonNode data = josnEntity.getData();
          Timestamp currentTime = new Timestamp(System.currentTimeMillis());
          if(data.get(Constants.IS_ACTIVE).asBoolean()){
            ((ObjectNode) data).put("isActive", false);
            josnEntity.setData(data);
            josnEntity.setDemandId(id);
            josnEntity.setUpdatedOn(currentTime);
            DemandEntity updateJsonEntity = demandRepository.save(josnEntity);
            Map<String, Object> map = objectMapper.convertValue(data, Map.class);
            esUtilService.addDocument(Constants.INDEX_NAME, "_doc", id, map);
            cacheService.putCache(id,data);
            return "Demand details deleted successfully.";
          }else
            return "demand is already inactive.";
        }else return "Demand not found.";
      } else return "Invalid entity ID.";
    } catch (Exception e) {
      return "Error deleting Entity with ID " + id + " " + e.getMessage();
    }
  }

  @Override
  public CustomResponse updateDemand(JsonNode demandsDetails) {
    log.info("DemandServiceImpl::updateDemand");
    if (demandsDetails.get(Constants.DEM_ID) == null) {
      throw new CustomException("ERROR", "SchemeDetailsEntity id is required for creating scheme record which is saved as draft");
    }
    log.info("Creating interest for demand with id : " + demandsDetails.get("id"));
    Optional<DemandEntity> optSchemeDetails = demandRepository.findById(demandsDetails.get(Constants.DEM_ID).asText());
    if (optSchemeDetails.isPresent()) {
      DemandEntity fetchedEntity = optSchemeDetails.get();
      ObjectNode combinedNode = objectMapper.createObjectNode();
      if(fetchedEntity.getData().get(Constants.INTEREST_COUNT).asInt() != 0){
        Object appendedInterest = fetchedEntity.getData().get(Constants.INTERESTS);
        ObjectNode interestsNode = (ObjectNode) demandsDetails.get(Constants.INTERESTS);
        interestsNode.setAll((ObjectNode) appendedInterest);
      }
      combinedNode.setAll((ObjectNode) optSchemeDetails.get().getData());
    }
    return null;
  }

  public void validatePayload(String fileName, JsonNode payload) {
    try {
      JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance();
      InputStream schemaStream = schemaFactory.getClass().getResourceAsStream(fileName);
      JsonSchema schema = schemaFactory.getSchema(schemaStream);

      Set<ValidationMessage> validationMessages = schema.validate(payload);
      if (!validationMessages.isEmpty()) {
        StringBuilder errorMessage = new StringBuilder("Validation error(s): \n");
        for (ValidationMessage message : validationMessages) {
          errorMessage.append(message.getMessage()).append("\n");
        }
        throw new CustomException(Constants.ERROR, errorMessage.toString());
      }
    } catch (CustomException e) {
      throw new CustomException(Constants.ERROR, "Failed to validate payload: " + e.getMessage());
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
    //response.getParams().setErrorMsg(errorMessage);
    response.getParams().setStatus(status);
    response.setResponseCode(httpStatus);
  }
}

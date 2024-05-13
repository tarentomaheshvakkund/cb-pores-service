package com.igot.cb.demand.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.igot.cb.demand.entity.DemandEntity;
import com.igot.cb.demand.repository.DemandRepository;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    private RedisTemplate<String, SearchResult> redisTemplate;

    @Value("${search.result.redis.ttl}")
    private long searchResultRedisTtl;

    @Override
    public CustomResponse createDemand(JsonNode demandDetails) {
        CustomResponse response = new CustomResponse();
        validatePayload(Constants.PAYLOAD_VALIDATION_FILE, demandDetails);
        try {
            log.info("DemandService::createDemand:creating demand");
            Random random = new Random();
            int randomNumber = 1000000 + random.nextInt(8999999);
            String id = String.valueOf(randomNumber);
            ((ObjectNode) demandDetails).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            ((ObjectNode) demandDetails).put(Constants.CREATED_ON, String.valueOf(currentTime));
            ((ObjectNode) demandDetails).put(Constants.UPDATED_ON, String.valueOf(currentTime));
            ((ObjectNode) demandDetails).put(Constants.INTEREST_COUNT, 0);

            DemandEntity jsonNodeEntity = new DemandEntity();
            jsonNodeEntity.setDemandId(id);
            jsonNodeEntity.setData(demandDetails);
            jsonNodeEntity.setCreatedOn(currentTime);
            jsonNodeEntity.setUpdatedOn(currentTime);

            DemandEntity saveJsonEntity = demandRepository.save(jsonNodeEntity);

            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode jsonNode = objectMapper.createObjectNode();
            jsonNode.set(Constants.DEMAND_ID, new TextNode(saveJsonEntity.getDemandId()));
            jsonNode.setAll((ObjectNode) saveJsonEntity.getData());

            Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
            esUtilService.addDocument(Constants.INDEX_NAME, "_doc", id, map);

            cacheService.putCache(jsonNodeEntity.getDemandId(), jsonNode);
            log.info("demand created");
            response.setMessage(Constants.SUCCESSFULLY_CREATED);
            map.put("demandId", id);
            response.setResult(map);
            response.setResponseCode(HttpStatus.OK);
            return response;
        } catch (Exception e) {
            throw new CustomException("error while processing", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    @Override
    public CustomResponse readDemand(String id) {
        log.info("reading demands for content");
        CustomResponse response = new CustomResponse();
        if (StringUtils.isEmpty(id)) {
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.setMessage("Id not found");
            return response;
        }
        try {
            String cachedJson = cacheService.getCache(id);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info("Record coming from redis cache");
                response.setMessage("successfully reading your data");
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
                    response.setMessage("successfully reading your data");
                    response
                            .getResult()
                            .put(Constants.RESULT,
                                    objectMapper.convertValue(
                                            demandEntity.getData(), new TypeReference<Object>() {
                                            }));
                } else {
                    response.setResponseCode(HttpStatus.NOT_FOUND);
                    response.setMessage("Invalid Id");
                }
            }
        } catch (JsonMappingException e) {
            throw new CustomException(Constants.ERROR, "error while processing", HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    @Override
    public CustomResponse searchDemand(SearchCriteria searchCriteria) {
        log.info("DemandServiceImpl::searchDemand");
        CustomResponse response = new CustomResponse();
        SearchResult searchResult = redisTemplate.opsForValue().get(generateRedisJwtTokenKey(searchCriteria));
        if (searchResult != null) {
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
                    HttpStatus.BAD_REQUEST,
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
            createErrorResponse(response, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR, Constants.FAILED_CONST);
            redisTemplate.opsForValue()
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
                    if (data.get(Constants.IS_ACTIVE).asBoolean()) {
                        ((ObjectNode) data).put("isActive", false);
                        josnEntity.setData(data);
                        josnEntity.setDemandId(id);
                        josnEntity.setUpdatedOn(currentTime);
                        DemandEntity updateJsonEntity = demandRepository.save(josnEntity);
                        Map<String, Object> map = objectMapper.convertValue(data, Map.class);
                        esUtilService.addDocument(Constants.INDEX_NAME, "_doc", id, map);
                        cacheService.putCache(id, data);
                        return "Demand details deleted successfully.";
                    } else
                        return "demand is already inactive.";
                } else return "Demand not found.";
            } else return "Invalid demand ID.";
        } catch (Exception e) {
            return "Error deleting demand with ID " + id + " " + e.getMessage();
        }
    }


    public CustomResponse updateDemand(JsonNode demandsDetails) {
        log.info("DemandServiceImpl::updateDemand");
        CustomResponse response = new CustomResponse();
        if (demandsDetails.get(Constants.DEMAND_ID) == null) {
            throw new CustomException(Constants.ERROR,"demandsDetailsEntity id is required for creating interest",HttpStatus.BAD_REQUEST);
        }
        log.info("Creating interest for demand with id : " + demandsDetails.get("id"));
        Optional<DemandEntity> optSchemeDetails = demandRepository.findById(demandsDetails.get(Constants.DEMAND_ID).asText());
        if (optSchemeDetails.isPresent()) {
            DemandEntity fetchedEntity = optSchemeDetails.get();
            JsonNode persistUpdatedDemand = fetchedEntity.getData();
            persistInPrimaryDb(fetchedEntity, demandsDetails);
            ObjectNode jsonNode = objectMapper.createObjectNode();
            jsonNode.set(Constants.DEMAND_ID, new TextNode(fetchedEntity.getDemandId()));
            jsonNode.setAll((ObjectNode) fetchedEntity.getData());

            Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
            esUtilService.addDocument(Constants.INDEX_NAME, "_doc", fetchedEntity.getDemandId(), map);

            cacheService.putCache(fetchedEntity.getDemandId(), jsonNode);
            log.info("interest captured");
            response.setMessage("Successfully created");
            response.setResponseCode(HttpStatus.OK);
            return response;
        } else {
            throw new CustomException(Constants.ERROR, Constants.No_DATA_FOUND, HttpStatus.NOT_FOUND);
        }

    }

    private void persistInPrimaryDb(DemandEntity fetchedEntity, JsonNode demandsDetails) {
        JsonNode persistUpdatedDemand = fetchedEntity.getData();
        // Retrieve existing interests
        JsonNode existingInterestsNode = fetchedEntity.getData().get(Constants.INTERESTS);

        // Create a new interest object for the new interest
        ObjectNode newInterestObject = objectMapper.createObjectNode();
        JsonNode payloadNode = demandsDetails.get(Constants.INTERESTS);
        payloadNode.fields().forEachRemaining(entry -> {
            newInterestObject.put(entry.getKey(), entry.getValue().asText());
        });

        // If existing interests is null or not an array, create a new array
        ArrayNode updatedInterestsArray = (existingInterestsNode != null && existingInterestsNode.isArray()) ?
                (ArrayNode) existingInterestsNode : objectMapper.createArrayNode();

        // Append the new interest object to the existing interests
        updatedInterestsArray.add(newInterestObject);

        // Update the fetched entity with the modified interests
        ((ObjectNode) fetchedEntity.getData()).set(Constants.INTERESTS, updatedInterestsArray);

        ((ObjectNode) persistUpdatedDemand).put(Constants.INTERESTS, (JsonNode) updatedInterestsArray);
        Timestamp currentTime = new Timestamp(System.currentTimeMillis());
        ((ObjectNode) persistUpdatedDemand).put(Constants.LAST_UPDATED_DATE, String.valueOf(currentTime));
        ((ObjectNode) persistUpdatedDemand).put(Constants.INTEREST_COUNT, persistUpdatedDemand.get(Constants.INTEREST_COUNT).asInt() + 1);
        ((ObjectNode) persistUpdatedDemand).put(Constants.DEMAND_ID, demandsDetails.get(Constants.DEMAND_ID));
        fetchedEntity.setData(persistUpdatedDemand);
        fetchedEntity.setUpdatedOn(currentTime);
        DemandEntity saveJsonEntity = demandRepository.save(fetchedEntity);
        log.info("persisted data in postgres");
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
                throw new CustomException("Validation Error", errorMessage.toString(), HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            throw new CustomException("Failed to validate payload", e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    public void createSuccessResponse(CustomResponse response) {
        response.setParams(new RespParam());
        response.getParams().setStatus("SUCCESS");
        response.setResponseCode(HttpStatus.OK);
    }

    public void createErrorResponse(
            CustomResponse response, String errorMessage, HttpStatus httpStatus, String status) {
        response.setParams(new RespParam());
        //response.getParams().setErrorMsg(errorMessage);
        response.getParams().setStatus(status);
        response.setResponseCode(httpStatus);
    }
}

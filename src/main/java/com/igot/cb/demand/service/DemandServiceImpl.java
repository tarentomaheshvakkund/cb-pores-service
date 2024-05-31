package com.igot.cb.demand.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.demand.entity.DemandEntity;
import com.igot.cb.demand.repository.DemandRepository;
import com.igot.cb.demand.util.StatusTransitionConfig;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.dto.RespParam;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.producer.Producer;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.transactional.model.Config;
import com.igot.cb.transactional.model.NotificationAsyncRequest;
import com.igot.cb.transactional.model.Template;
import com.igot.cb.transactional.service.RequestHandlerServiceImpl;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
    private Logger logger = LoggerFactory.getLogger(DemandServiceImpl.class);
    @Autowired
    private AccessTokenValidator accessTokenValidator;
    private StatusTransitionConfig statusTransitionConfig;

    @Autowired
    public DemandServiceImpl() throws IOException {
        this.statusTransitionConfig = new StatusTransitionConfig(Constants.STATUS_TRANSITION_PATH);
    }

    @Autowired
    private CassandraOperation cassandraOperation;
    @Value("${search.result.redis.ttl}")
    private long searchResultRedisTtl;

    @Autowired
    private Producer kafkaProducer;

    @Autowired
    private CbServerProperties propertiesConfig;

    @Autowired
    private RequestHandlerServiceImpl requestHandlerService;

    @Override
    public CustomResponse createDemand(JsonNode demandDetails, String token, String rootOrgId) {
        log.info("DemandService::createDemand:creating demand");
        CustomResponse response = new CustomResponse();
        validatePayload(Constants.PAYLOAD_VALIDATION_FILE, demandDetails);
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        validateUser(rootOrgId, response, userId);
        if (response.getResponseCode() != HttpStatus.OK) {
            return response;
        }
        try {
            String id = generateUniqueDemandId();
            ((ObjectNode) demandDetails).put(Constants.IS_ACTIVE, Constants.ACTIVE_STATUS);
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            ((ObjectNode) demandDetails).put(Constants.CREATED_ON, String.valueOf(currentTime));
            ((ObjectNode) demandDetails).put(Constants.UPDATED_ON, String.valueOf(currentTime));
            ((ObjectNode) demandDetails).put(Constants.INTEREST_COUNT, 0);
            ((ObjectNode) demandDetails).put(Constants.OWNER, userId);
            ((ObjectNode) demandDetails).put(Constants.ROOT_ORG_ID, rootOrgId);
            String requestType = demandDetails.get(Constants.REQUEST_TYPE).asText();
            if (requestType.equals(Constants.BROADCAST)) {
                ((ObjectNode) demandDetails).put(Constants.STATUS, Constants.UNASSIGNED);
            } else {
                ((ObjectNode) demandDetails).put(Constants.STATUS, Constants.ASSIGNED);
            }
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
            //esUtilService.addDocument(Constants.INDEX_NAME, Constants.INDEX_TYPE, id, map);

            //cacheService.putCache(jsonNodeEntity.getDemandId(), jsonNode);
            log.info("demand created successfully");
            boolean isSpvReq = false;
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put(Constants.DATA,map);
            dataMap.put(Constants.IS_SPV_REQUEST,isSpvReq);
            dataMap.put(Constants.USER_ID_RQST,userId);
            if(isSpvRequest(userId)){
                dataMap.put(Constants.IS_SPV_REQUEST,true);
            }
            if (map.get(Constants.REQUEST_TYPE).equals(Constants.BROADCAST) && ObjectUtils.isNotEmpty(map.get(Constants.PREFERRED_PROVIDER))) {
                kafkaProducer.push(propertiesConfig.getDemandRequestKafkaTopic(), dataMap);
            }
            if(map.get(Constants.REQUEST_TYPE).equals(Constants.SINGLE)){
                kafkaProducer.push(propertiesConfig.getDemandRequestKafkaTopic(), dataMap);
            }
            response.setMessage(Constants.SUCCESSFULLY_CREATED);
            map.put(Constants.DEMAND_ID, id);
            response.setResult(map);
            response.setResponseCode(HttpStatus.OK);
            return response;
        } catch (Exception e) {
            logger.error("Error occurred while creating demand", e);
            response.getParams().setErrmsg("error while processing");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }


    @Override
    public CustomResponse readDemand(String id) {
        log.info("reading demands for content");
        CustomResponse response = new CustomResponse();
        if (StringUtils.isEmpty(id)) {
            logger.error("Id not found");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.setMessage(Constants.ID_NOT_FOUND);
            return response;
        }
        try {
            String cachedJson = cacheService.getCache(id);
            if (StringUtils.isNotEmpty(cachedJson)) {
                log.info("Record coming from redis cache");
                response.setMessage(Constants.SUCCESSFULLY_READING);
                response.getResult().put(Constants.RESULT, objectMapper.readValue(cachedJson, new TypeReference<Object>() {
                }));
            } else {
                Optional<DemandEntity> entityOptional = demandRepository.findById(id);
                if (entityOptional.isPresent()) {
                    DemandEntity demandEntity = entityOptional.get();
                    cacheService.putCache(id, demandEntity.getData());
                    log.info("Record coming from postgres db");
                    response.setMessage(Constants.SUCCESSFULLY_READING);
                    response.getResult().put(Constants.RESULT, objectMapper.convertValue(demandEntity.getData(), new TypeReference<Object>() {
                    }));
                } else {
                    logger.error("Invalid Id: {}", id);
                    response.setResponseCode(HttpStatus.NOT_FOUND);
                    response.setMessage(Constants.INVALID_ID);
                }
            }
        } catch (Exception e) {
            logger.error("Error while mapping JSON for id {}: {}", id, e.getMessage(), e);
            throw new CustomException(Constants.ERROR, "error while processing", HttpStatus.INTERNAL_SERVER_ERROR);
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
            createErrorResponse(response, "Minimum 3 characters are required to search", HttpStatus.BAD_REQUEST, Constants.FAILED_CONST);
            return response;
        }
        try {
            searchResult = esUtilService.searchDocuments(Constants.INDEX_NAME, searchCriteria);
            response.getResult().put(Constants.RESULT, searchResult);
            createSuccessResponse(response);
            return response;
        } catch (Exception e) {
            createErrorResponse(response, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR, Constants.FAILED_CONST);
            redisTemplate.opsForValue().set(generateRedisJwtTokenKey(searchCriteria), searchResult, searchResultRedisTtl, TimeUnit.SECONDS);
            return response;
        }
    }

    public String generateRedisJwtTokenKey(Object requestPayload) {
        if (requestPayload != null) {
            try {
                String reqJsonString = objectMapper.writeValueAsString(requestPayload);
                return JWT.create().withClaim(Constants.REQUEST_PAYLOAD, reqJsonString).sign(Algorithm.HMAC256(Constants.JWT_SECRET_KEY));
            } catch (JsonProcessingException e) {
                logger.error("Error occurred while converting json object to json string", e);
            }
        }
        return "";
    }

    @Override
    public String delete(String id) {
        log.info("DemandServiceImpl::delete Demand");
        try {
            if (StringUtils.isNotEmpty(id)) {
                Optional<DemandEntity> entityOptional = demandRepository.findById(id);
                if (entityOptional.isPresent()) {
                    DemandEntity josnEntity = entityOptional.get();
                    JsonNode data = josnEntity.getData();
                    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                    if (data.get(Constants.IS_ACTIVE).asBoolean()) {
                        ((ObjectNode) data).put(Constants.IS_ACTIVE, false);
                        ((ObjectNode) data).put(Constants.UPDATED_ON, String.valueOf(currentTime));
                        ((ObjectNode) data).put(Constants.DEMAND_ID, id);
                        josnEntity.setData(data);
                        josnEntity.setDemandId(id);
                        josnEntity.setUpdatedOn(currentTime);
                        DemandEntity updateJsonEntity = demandRepository.save(josnEntity);
                        Map<String, Object> map = objectMapper.convertValue(data, Map.class);
                        esUtilService.addDocument(Constants.INDEX_NAME, Constants.INDEX_TYPE, id, map);
                        cacheService.putCache(id, data);

                        logger.debug("Demand details deleted successfully");
                        return Constants.DELETED_SUCCESSFULLY;
                    } else log.info("demand is already inactive.");
                    return Constants.ALREADY_INACTIVE;
                } else return Constants.NO_DATA_FOUND;
            } else return Constants.INVALID_ID;
        } catch (Exception e) {
            logger.error("Error while deleting demand with ID: {}. Exception: {}", id, e.getMessage(), e);
            return Constants.ERROR_WHILE_DELETING_DEMAND + id + " " + e.getMessage();
        }
    }

    @Override
    public CustomResponse updateDemandStatus(JsonNode updateDetails, String token, String rootOrgId) {
        log.info("DemandServiceImpl::updateDemandStatus");
        CustomResponse response = new CustomResponse();
        String userId = accessTokenValidator.verifyUserToken(token);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        //validateUser(rootOrgId, response, userId);
        /*if (response.getResponseCode() != HttpStatus.OK) {
            return response;
        }*/
        if (!updateDetails.has(Constants.DEMAND_ID) || StringUtils.isEmpty(updateDetails.get(Constants.DEMAND_ID).asText(null))
                || !updateDetails.has(Constants.NEW_STATUS) || StringUtils.isEmpty(updateDetails.get(Constants.NEW_STATUS).asText(null))) {
            logger.error("demand id and newStatus are required for updating demand");
            throw new CustomException(Constants.ERROR, Constants.MISSING_ID_OR_NEW_STATUS, HttpStatus.BAD_REQUEST);
        }
        try {
            log.info("updating demand status with id : " + updateDetails.get("id"));
            Optional<DemandEntity> optionalDemand = demandRepository.findById(updateDetails.get(Constants.DEMAND_ID).asText());
            if (optionalDemand.isPresent()) {
                DemandEntity demandDbData = optionalDemand.get();
                JsonNode data = demandDbData.getData();
                String currentStatus = data.get(Constants.STATUS).asText();
                String requestType = data.get(Constants.REQUEST_TYPE).asText();
                boolean isActive = data.get(Constants.IS_ACTIVE).asBoolean();
                String newStatus = updateDetails.get(Constants.NEW_STATUS).asText();

                if (!isActive) {
                    logger.error("You are trying to update an inactive demand");
                    throw new CustomException(Constants.ERROR, Constants.CANNOT_UPDATE_INACTIVE_DEMAND, HttpStatus.BAD_REQUEST);
                }
                /*if (!statusTransitionConfig.isValidTransition(requestType, currentStatus, newStatus)) {
                    logger.error("Invalid Status transition", newStatus);
                    throw new CustomException(Constants.ERROR, Constants.INVALID_STATUS_TRANSITION, HttpStatus.BAD_REQUEST);
                }*/
                // Update the status
                ((ObjectNode) data).put(Constants.STATUS, newStatus);
                demandDbData.setData(data);
                Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                ((ObjectNode) data).put(Constants.UPDATED_ON, String.valueOf(currentTime));
                demandDbData.setUpdatedOn(currentTime);

                DemandEntity saveJsonEntity = demandRepository.save(demandDbData);

                ObjectMapper objectMapper = new ObjectMapper();
                ObjectNode jsonNode = objectMapper.createObjectNode();
                jsonNode.set(Constants.DEMAND_ID, new TextNode(saveJsonEntity.getDemandId()));
                jsonNode.setAll((ObjectNode) saveJsonEntity.getData());
                Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
                esUtilService.addDocument(Constants.INDEX_NAME, Constants.INDEX_TYPE, saveJsonEntity.getDemandId(), map);

                cacheService.putCache(saveJsonEntity.getDemandId(), jsonNode);
                log.info("demand updated");
                Map<String,Object> notifcationMap = new HashMap<>();
                notifcationMap.put(Constants.DATA,map);
                notifcationMap.put(Constants.USER_ID_RQST,userId);
                notifcationMap.put(Constants.IS_SPV_REQUEST,false);
                if(map.get(Constants.STATUS).equals(Constants.INVALID)){
                    notifcationMap.put(Constants.IS_SPV_REQUEST,true); //isSpvRequest(userId)
                    processNotification(notifcationMap);
                    //kafkaProducer.push(propertiesConfig.getDemandRequestKafkaTopic(),notifcationMap);
                }
                response.setMessage(Constants.SUCCESSFULLY_UPDATED);
                map.put(Constants.DEMAND_ID, saveJsonEntity.getDemandId());
                response.setResult(map);
                response.setResponseCode(HttpStatus.OK);
            } else {
                logger.error("Demand Data not Found with this ID");
                throw new CustomException(Constants.ERROR, Constants.INVALID_ID, HttpStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            logger.error("Error occurred while updating demand status", e);
            throw new CustomException("Error occurred while updating demand status", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
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
                logger.error("Validation Error", errorMessage.toString());
                throw new CustomException("Validation Error", errorMessage.toString(), HttpStatus.BAD_REQUEST);
            }
        } catch (Exception e) {
            logger.error("Failed to validate payload", e);
            throw new CustomException("Failed to validate payload", e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    public CustomResponse validateUser(String rootOrgId, CustomResponse response, String userId) {
        log.info("Validating the user with rootOrgId: {} and userId: {}", rootOrgId, userId);
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ID, userId);

            List<Map<String, Object>> userDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER, propertyMap,
                    Arrays.asList(Constants.ROOT_ORG_ID), 2);

            String userRootOrgId = null;
            if (!CollectionUtils.isEmpty(userDetails)) {
                userRootOrgId = (String) userDetails.get(0).get(Constants.USER_ROOT_ORG_ID);
            } else {
                log.error("User details not found in Cassandra for validating user{}", userId);
                response.getParams().setErrmsg("User details not found with userId");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (!rootOrgId.equals(userRootOrgId)) {
                log.warn("Root org ID does not match Expected: {}, Found: {}", rootOrgId, userRootOrgId);
                response.getParams().setErrmsg(Constants.ROOT_ORG_ID_DOESNT_MATCH);
                response.setResponseCode(HttpStatus.FORBIDDEN);
                return response;
            }
        } catch (Exception e) {
            logger.error("An error occurred while validating user root org ID.", e);
            response.getParams().setErrmsg("An error occurred while validating user root org ID.");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        response.setResponseCode(HttpStatus.OK);
        return response;
    }

    public String generateUniqueDemandId() {
        String id;
        boolean idExists;
        int attempts = 0;
        int MAX_ATTEMPTS = 100;
        int digitLength = 7;
        long totalIds = demandRepository.count();

        while (totalIds >= Math.pow(10, digitLength)) {
            digitLength++;
        }
        Random random = new Random();
        do {
            ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault());
            int secondsInDay = now.getHour() * 3600 + now.getMinute() * 60 + now.getSecond();
            int randomPartLength = digitLength - 5;
            int maxRandomValue = (int) Math.pow(10, randomPartLength);
            int randomNumber = random.nextInt(maxRandomValue);
            id = String.format("%05d%" + String.format("0%dd", randomPartLength), secondsInDay, randomNumber);

            idExists = demandRepository.existsById(id);
            log.info("DemandService::generateUniqueDemandId: generated id={} exists={}", id, idExists);
            attempts++;
        } while (idExists && attempts < MAX_ATTEMPTS);
        if (idExists) {
            throw new RuntimeException("Unable to generate a unique ID after " + MAX_ATTEMPTS + " attempts");
        }
        return id;
    }

    public void createSuccessResponse(CustomResponse response) {
        response.setParams(new RespParam());
        response.getParams().setStatus(Constants.SUCCESS);
        response.setResponseCode(HttpStatus.OK);
    }

    public void createErrorResponse(CustomResponse response, String errorMessage, HttpStatus httpStatus, String status) {
        response.setParams(new RespParam());
        //response.getParams().setErrorMsg(errorMessage);
        response.getParams().setStatus(status);
        response.setResponseCode(httpStatus);
    }
    public boolean isSpvRequest(String userId) {
        Map<String, String> header = new HashMap<>();
        Map<String, Object> readData = (Map<String, Object>) requestHandlerService
                .fetchUsingGetWithHeadersProfile(propertiesConfig.getSbUrl() + propertiesConfig.getUserReadEndPoint() + userId,
                        header);
        Map<String, Object> result = (Map<String, Object>) readData.get(Constants.RESULT);
        Map<String, Object> responseMap = (Map<String, Object>) result.get(Constants.RESPONSE);
        List roles = (List) responseMap.get("roles");

        if(roles.contains("SPV_ADMIN")){
            return true;
        }else return false;
    }

    public void processNotification(Map<String,Object> demandRequest){
        try {
            long startTime = System.currentTimeMillis();
            Map<String,Object> request = (Map<String, Object>) demandRequest.get(Constants.DATA);
            String status = (String) request.get(Constants.STATUS);
            boolean isSpvRequest = (boolean) (demandRequest.get(Constants.IS_SPV_REQUEST));
            /*if(!(request.get(Constants.OWNER).equals(demandRequest.get(Constants.USER_ID_RQST)))){

            }*/

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ID, (String)request.get(Constants.ROOT_ORG_ID));
            List<Map<String, Object>> orgDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD, Constants.ORG_TABLE, propertyMap, null, 1);
            String mdoName = "Shankar Mdo"; //(String) orgDetails.get(0).get(Constants.USER_ROOT_ORG_NAME);

            String subjectLine = "";
            String body = "";
            Map<String, Object> mailNotificationDetails = new HashMap<>();
            switch (status){
                case Constants.UNASSIGNED:
                    subjectLine = Constants.REQUEST_CONTENT_SUBJECT.replace(Constants.DEMAND_TAG,(String)request.get(Constants.DEMAND_ID));
                    body = Constants.PREFERRED_MAIL_BODY.replace(Constants.DEMAND_TAG,(String)request.get(Constants.DEMAND_ID)).replace(Constants.MDO_NAME_TAG,mdoName);
                    break;
                case Constants.ASSIGNED:
                    subjectLine = Constants.DEMAND_ASSIGNED_SUB.replace(Constants.DEMAND_TAG,(String)request.get(Constants.DEMAND_ID));
                    body = Constants.ASSIGNED_MAIL_BODY_TO_CBP.replace(Constants.DEMAND_TAG,(String)((String) request.get(Constants.DEMAND_ID)).replace(Constants.MDO_NAME_TAG,mdoName));
                    break;
            }

            List<String> emails = new ArrayList<>();
            if(status.equals(Constants.UNASSIGNED)) {
                List<Map<String, Object>> recipientsList = (List<Map<String, Object>>) request.get(Constants.PREFERRED_PROVIDER);
                List<String> providerIds = new ArrayList<>();
                for (Map<String, Object> recipient : recipientsList) {
                    providerIds.add((String) recipient.get(Constants.PROVIDER_ID));
                }
                emails = getCBPAdminDetails((Set<String>) providerIds);
            }
            if(status.equals(Constants.ASSIGNED)){
                Map<String, Object> assignedProvider = (Map<String, Object>) request.get(Constants.ASSIGNED_PROVIDER);
                emails =  getCBPAdminDetails((Set<String>) assignedProvider.get(Constants.PROVIDER_ID));

            }/*if(status.equals(Constants.INVALID)){

            }*/

            List<Map<String, String>> competencies = (List<Map<String, String>>) request.get(Constants.COMPETENCIES);
            StringBuilder allArea = new StringBuilder();
            StringBuilder allThemes = new StringBuilder();
            StringBuilder allSubThemes = new StringBuilder();
            for (Map<String, String> theme : competencies) {
                allArea.append(theme.get(Constants.AREA)).append(", ");
                allThemes.append(theme.get(Constants.THEME)).append(", ");
                allSubThemes.append(theme.get(Constants.SUB_THEME)).append(", ");
            }

            mailNotificationDetails.put(Constants.EMAIL_ID_LIST, emails);
            mailNotificationDetails.put(Constants.MDO_NAME, request.get(Constants.MDO_NAME));
            mailNotificationDetails.put(Constants.COMPETENCY_AREA, allArea.replace(allArea.length() - 2, allArea.length() - 1, "."));
            mailNotificationDetails.put(Constants.COMPETENCY_THEMES, allThemes.replace(allThemes.length() - 2, allThemes.length() - 1, "."));
            mailNotificationDetails.put(Constants.COMPETENCY_SUB_THEMES, allSubThemes.replace(allSubThemes.length() - 2, allSubThemes.length() - 1, "."));
            mailNotificationDetails.put(Constants.DESCRIPTION , request.get(Constants.DESCRIPTION));
            mailNotificationDetails.put(Constants.CREATED_BY, mdoName);
            mailNotificationDetails.put(Constants.DEMAND_ID, request.get(Constants.DEMAND_ID));
            mailNotificationDetails.put(Constants.SUB,subjectLine);
            mailNotificationDetails.put(Constants.BODY, body);
            if (request.get(Constants.STATUS).equals(Constants.ASSIGNED) ||
                    request.get(Constants.STATUS).equals(Constants.UNASSIGNED)) {
                sendNotificationToProvidersAsync(mailNotificationDetails);
            }

            if(status.equals(Constants.ASSIGNED)&& isSpvRequest){
                Map<String, Object> assignedProvider = (Map<String, Object>) request.get(Constants.ASSIGNED_PROVIDER);
                emails = fetchEmailFromUserId(Collections.singletonList((String) request.get(Constants.OWNER)));
                subjectLine = Constants.REQUEST_CONTENT_SUBJECT.replace(Constants.DEMAND_TAG,(String)request.get(Constants.DEMAND_ID));
                body = Constants.ASSIGNED_MAIL_BODY_TO_MDO.replace(Constants.DEMAND_TAG,(String)request.get(Constants.DEMAND_ID)).replace(Constants.CONTENT_PROVIDER_NAME_TAG,(String) assignedProvider.get(Constants.PROVIDER_NAME));
                mailNotificationDetails.put(Constants.SUB,subjectLine);
                mailNotificationDetails.put(Constants.BODY, body);
                mailNotificationDetails.put(Constants.EMAIL_ID_LIST, emails);
                sendNotificationToProvidersAsync(mailNotificationDetails);
            }
            if (status.equals(Constants.INVALID)&&isSpvRequest){
                emails = fetchEmailFromUserId(Collections.singletonList((String) request.get(Constants.OWNER))); //Collections.singletonList("useradarsh@yopmail.com");
                subjectLine = Constants.DEMAND_INVALID_SUB.replace(Constants.DEMAND_TAG,(String)request.get(Constants.DEMAND_ID));
                body = Constants.INVALID_DEMAND_MAIL_BODY_MDO.replace(Constants.DEMAND_TAG,(String)request.get(Constants.DEMAND_ID));
                mailNotificationDetails.put(Constants.EMAIL_ID_LIST, emails);
                mailNotificationDetails.put(Constants.SUB,subjectLine);
                mailNotificationDetails.put(Constants.BODY, body);
                sendNotificationToProvidersAsync(mailNotificationDetails);
            }
            logger.info(String.format("Completed request for content. Time taken: ", (System.currentTimeMillis() - startTime)));
        } catch (Exception e) {
            logger.error("Exception occurred while sending email : " + e.getMessage(), e);
        }
    }

    private String constructEmailTemplate(String templateName, Map<String, Object> params) {
        String replacedHTML = new String();
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.NAME, templateName);
            List<Map<String, Object>> templateMap = cassandraOperation.getRecordsByPropertiesByKey(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_EMAIL_TEMPLATE, propertyMap, Collections.singletonList(Constants.TEMPLATE),null);
            String htmlTemplate = templateMap.stream()
                    .findFirst()
                    .map(template -> (String) template.get(Constants.TEMPLATE))
                    .orElse(null);
            VelocityEngine velocityEngine = new VelocityEngine();
            velocityEngine.init();
            VelocityContext context = new VelocityContext();
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                context.put(entry.getKey(), entry.getValue());
            }
            StringWriter writer = new StringWriter();
            velocityEngine.evaluate(context, writer, "HTMLTemplate", htmlTemplate);
            replacedHTML = writer.toString();
        } catch (Exception e) {
            logger.error("Unable to create template ", e);
        }
        return replacedHTML;
    }

    private void sendNotificationToProvidersAsync(Map<String, Object> mailNotificationDetails){
        Map<String, Object> params = new HashMap<>();
        NotificationAsyncRequest notificationAsyncRequest = new NotificationAsyncRequest();
        notificationAsyncRequest.setPriority(1);
        notificationAsyncRequest.setType(Constants.EMAIL);
        notificationAsyncRequest.setIds((List<String>) mailNotificationDetails.get(Constants.EMAIL_ID_LIST));

        params.put(Constants.MDO_NAME_PARAM, (String) mailNotificationDetails.get(Constants.MDO_NAME));
        params.put(Constants.NAME, (String) mailNotificationDetails.get(Constants.MDO_NAME));
        params.put(Constants.COMPETENCY_AREA_PARAM, mailNotificationDetails.get(Constants.COMPETENCY_AREA));
        params.put(Constants.COMPETENCY_THEME_PARAM, mailNotificationDetails.get(Constants.COMPETENCY_THEMES));
        params.put(Constants.COMPETENCY_SUB_THEME_PARAM, mailNotificationDetails.get(Constants.COMPETENCY_SUB_THEMES));
        params.put(Constants.DESCRIPTION, mailNotificationDetails.get(Constants.DESCRIPTION));
        params.put(Constants.FROM_EMAIL, propertiesConfig.getSupportEmail());
        params.put(Constants.ORG_NAME, (String) mailNotificationDetails.get(Constants.MDO_NAME));
        params.put(Constants.BODY,mailNotificationDetails.get(Constants.BODY));
        Template template = new Template(constructEmailTemplate(propertiesConfig.getDemandRequestTemplate(), params),propertiesConfig.getDemandRequestTemplate(), params);

        Config config = new Config();
        config.setSubject((String) mailNotificationDetails.get(Constants.SUB));
        config.setSender(propertiesConfig.getSupportEmail());

        Map<String, Object> templateMap = new HashMap<>();
        templateMap.put(Constants.CONFIG, config);
        templateMap.put(Constants.TYPE, Constants.EMAIL);
        templateMap.put(Constants.DATA, template.getData());
        templateMap.put(Constants.ID, propertiesConfig.getDemandRequestTemplate());
        templateMap.put(Constants.PARAMS, params);

        Map<String, Object> action = new HashMap<>();
        action.put(Constants.TEMPLATE, templateMap);
        action.put(Constants.TYPE, Constants.EMAIL);
        action.put(Constants.CATEGORY, Constants.EMAIL);

        Map<String, Object> createdBy = new HashMap<>();
        createdBy.put(Constants.ID, mailNotificationDetails.get(Constants.CREATED_BY));
        createdBy.put(Constants.TYPE, Constants.MDO);
        action.put(Constants.CREATED_BY, createdBy);
        notificationAsyncRequest.setAction(action);

        Map<String, Object> req = new HashMap<>();
        Map<String, List<NotificationAsyncRequest>> notificationMap = new HashMap<>();
        notificationMap.put(Constants.NOTIFICATIONS, Collections.singletonList(notificationAsyncRequest));
        req.put(Constants.REQUEST, notificationMap);
        sendNotification(req, propertiesConfig.getNotificationAsyncPath());
    }

    private void sendNotification(Map<String, Object> request, String urlPath) {
        StringBuilder builder = new StringBuilder();
        builder.append(propertiesConfig.getNotifyServiceHost()).append(urlPath);
        try {
            logger.info(objectMapper.writeValueAsString(request));
            Map<String, Object> response = requestHandlerService.fetchResultUsingPost(builder.toString(), request, null);
            logger.debug("The email notification is successfully sent, response is: " + response);
        } catch (Exception e) {
            logger.error("Exception while posting the data in notification service: ", e);
        }
    }

    public List<String> fetchEmails(List<String> ids){

        Map<String, Object> requestObject = new HashMap<>();
        Map<String, Object> req = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.USER_ID_RQST, ids);
        List<String> userFields = Arrays.asList(Constants.USER_ID_RQST, Constants.PROFILE_DETAILS);
        req.put(Constants.FILTERS, filters);
        req.put(Constants.FIELDS, userFields);
        requestObject.put(Constants.REQUEST, req);
        HashMap<String, String> headersValue = new HashMap<>();
        headersValue.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        StringBuilder url = new StringBuilder(propertiesConfig.getSbUrl()).append(propertiesConfig.getUserSearchEndPoint());
        Map<String, Object> searchProfileApiResp = requestHandlerService.fetchResultUsingPost(url.toString(), requestObject, headersValue);
        List<String> emailIds = new ArrayList<>();
        if (searchProfileApiResp != null
                && Constants.OK.equalsIgnoreCase((String) searchProfileApiResp.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> Map = (Map<String, Object>) searchProfileApiResp.get(Constants.RESULT);
            Map<String, Object> resp = (Map<String, Object>) Map.get(Constants.RESPONSE);
            List<Map<String, Object>> contents = (List<Map<String, Object>>) resp.get(Constants.CONTENT);
            for (Map<String, Object> content : contents) {
                Map<String, Object> profileDetails = (Map<String, Object>) content.get(Constants.PROFILE_DETAILS);
                Map<String, Object> personalDetails = (Map<String, Object>) profileDetails.get(Constants.PERSONAL_DETAILS);
                String email = (String) personalDetails.get(Constants.PRIMARY_EMAIL);
                emailIds.add(email);
            }
        }
        return emailIds;
    }

    private Map<String, Object> getSearchObject(Set<String> rootOrgIds) {
        Map<String, Object> requestObject = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.ROOT_ORG_ID, rootOrgIds);
        filters.put(Constants.ORGANIZATIONS_ROLES, Collections.singletonList(Constants.CBP_ADMIN));
        request.put(Constants.FILTERS, filters);
        request.put(Constants.FIELDS_CONSTANT, Arrays.asList("profileDetails.personalDetails.primaryEmail", Constants.ROOT_ORG_ID));
        requestObject.put(Constants.REQUEST, request);
        return requestObject;
    }

    public List<String> getCBPAdminDetails(Set<String> rootOrgIds) throws Exception {
        Map<String, Object> request = getSearchObject(rootOrgIds);
        HashMap<String, String> headersValue = new HashMap<>();
        headersValue.put("Content-Type", "application/json");
        List<String> providerIdEmails = new ArrayList<>();
        StringBuilder url = new StringBuilder(propertiesConfig.getSbUrl())
                .append(propertiesConfig.getUserSearchEndPoint());
        Map<String, Object> searchProfileApiResp = requestHandlerService.fetchResultUsingPost(url.toString(), request,
                headersValue);
        if (searchProfileApiResp != null
                && "OK".equalsIgnoreCase((String) searchProfileApiResp.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> map = (Map<String, Object>) searchProfileApiResp.get(Constants.RESULT);
            Map<String, Object> response = (Map<String, Object>) map.get(Constants.RESPONSE);
            List<Map<String, Object>> contents = (List<Map<String, Object>>) response.get(Constants.CONTENT);
            if (!org.springframework.util.CollectionUtils.isEmpty(contents)) {
                for (Map<String, Object> content : contents) {
                    String rootOrgId = (String) content.get(Constants.ROOT_ORG_ID);
                    HashMap<String, Object> profileDetails = (HashMap<String, Object>) content
                            .get(Constants.PROFILE_DETAILS);
                    if (!org.springframework.util.CollectionUtils.isEmpty(profileDetails)) {
                        HashMap<String, Object> personalDetails = (HashMap<String, Object>) profileDetails
                                .get(Constants.PERSONAL_DETAILS);
                        if (!org.springframework.util.CollectionUtils.isEmpty(personalDetails)
                                && personalDetails.get(Constants.PRIMARY_EMAIL) != null) {
                            if (rootOrgIds.contains(rootOrgId))
                                providerIdEmails.add((String) personalDetails.get(Constants.PRIMARY_EMAIL));
                        }
                    }
                }
            }
        }
        if (org.springframework.util.CollectionUtils.isEmpty(providerIdEmails)) {
            throw new Exception("Failed to find CBP Admin for OrgIds: " + rootOrgIds);
        }
        logger.info("CBP Admin emails fetched successfully: " + providerIdEmails);
        return providerIdEmails;
    }

    public List<String> fetchEmailFromUserId (List<String> userIds) {
        Map<String, Object> requestObject = new HashMap<>();
        Map<String, Object> req = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.USER_ID_RQST, userIds);
        List<String> userFields = Arrays.asList(Constants.PROFILE_DETAILS_PRIMARY_EMAIL);
        req.put(Constants.FILTERS, filters);
        req.put(Constants.FIELDS, userFields);
        requestObject.put(Constants.REQUEST, req);
        HashMap<String, String> headersValue = new HashMap<>();
        headersValue.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        StringBuilder url = new StringBuilder(propertiesConfig.getSbUrl()).append(propertiesConfig.getUserSearchEndPoint());
        Map<String, Object> searchProfileApiResp = requestHandlerService.fetchResultUsingPost(url.toString(), requestObject, headersValue);
        List<String> emailResponseList = new ArrayList<>();
        if (searchProfileApiResp != null
                && Constants.OK.equalsIgnoreCase((String) searchProfileApiResp.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> map = (Map<String, Object>) searchProfileApiResp.get(Constants.RESULT);
            Map<String, Object> resp = (Map<String, Object>) map.get(Constants.RESPONSE);
            List<Map<String, Object>> contents = (List<Map<String, Object>>) resp.get(Constants.CONTENT);
            for (Map<String, Object> content : contents) {
                Map<String, Object> profileDetails = (Map<String, Object>) content.get(Constants.PROFILE_DETAILS);
                Map<String, Object> personalDetails = (Map<String, Object>) profileDetails.get(Constants.PERSONAL_DETAILS);
                String email = (String) personalDetails.get(Constants.PRIMARY_EMAIL);
                emailResponseList.add(email);
            }
        }
        return emailResponseList;
    }
}

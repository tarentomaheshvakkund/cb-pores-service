package com.igot.cb.competencies.theme.service.impl;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.authentication.util.AccessTokenValidator;
import com.igot.cb.competencies.theme.enity.CompetencyThemeEntity;
import com.igot.cb.competencies.theme.repository.CompetencyThemeRepository;
import com.igot.cb.competencies.theme.service.CompetencyThemeService;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.dto.RespParam;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.FileProcessService;
import com.igot.cb.pores.util.PayloadValidation;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class CompetencyThemeServiceImpl implements CompetencyThemeService {

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  private PayloadValidation payloadValidation;

  @Autowired
  private EsUtilService esUtilService;

  @Autowired
  private CacheService cacheService;

  @Autowired
  private CbServerProperties cbServerProperties;

  @Autowired
  private FileProcessService fileProcessService;

  @Autowired
  private AccessTokenValidator accessTokenValidator;

  @Value("${search.result.redis.ttl}")
  private long searchResultRedisTtl;

  @Autowired
  private RedisTemplate<String, SearchResult> redisTemplate;

  @Autowired
  private CompetencyThemeRepository competencyThemeRepository;

  @Override
  public void loadCompetencyTheme(MultipartFile file, String token) {

    log.info("CompetencyThemeService::loadCompetencyThemeFromExcel");
    String userId = accessTokenValidator.verifyUserToken(token);
    if (!StringUtils.isBlank(userId)){
      List<Map<String, String>> processedData = fileProcessService.processExcelFile(file);
      log.info("No.of processedData from excel: " + processedData.size());
      JsonNode jsonNode = objectMapper.valueToTree(processedData);
      AtomicLong startingId = new AtomicLong(competencyThemeRepository.count());
      CompetencyThemeEntity compThemeEntity = new CompetencyThemeEntity();
      jsonNode.forEach(
          eachCompTheme -> {
            if (eachCompTheme.has(Constants.COMPETENCY_THEME_TYPE)){
              if (!eachCompTheme.get(
                  Constants.COMPETENCY_THEME_TYPE).asText().isEmpty()){
                String formattedId = String.format("COMTHEME-%06d", startingId.incrementAndGet());
                JsonNode dataNode = objectMapper.createObjectNode();
                ((ObjectNode) dataNode).put(Constants.ID, formattedId);
                ((ObjectNode) dataNode).put(Constants.TITLE, eachCompTheme.get(Constants.COMPETENCY_THEME_TYPE).asText());
                String descriptionValue =
                    (eachCompTheme.has(Constants.DESCRIPTION_PAYLOAD) && !eachCompTheme.get(
                        Constants.DESCRIPTION_PAYLOAD).isNull())
                        ? eachCompTheme.get(Constants.DESCRIPTION).asText()
                        : "";
                ((ObjectNode) dataNode).put(Constants.DESCRIPTION, descriptionValue);
                ((ObjectNode) dataNode).put(Constants.STATUS, Constants.LIVE);
                Timestamp currentTime = new Timestamp(System.currentTimeMillis());
                ((ObjectNode) dataNode).put(Constants.CREATED_ON, String.valueOf(currentTime));
                ((ObjectNode) dataNode).put(Constants.UPDATED_ON, String.valueOf(currentTime));
                ((ObjectNode) dataNode).put(Constants.CREATED_BY, userId);
                ((ObjectNode) dataNode).put(Constants.VERSION, 1);
                payloadValidation.validatePayload(Constants.COMP_AREA_PAYLOAD_VALIDATION,
                    dataNode);
                List<String> searchTags = new ArrayList<>();
                searchTags.add(dataNode.get(Constants.TITLE).textValue().toLowerCase());
                ArrayNode searchTagsArray = objectMapper.valueToTree(searchTags);
                ((ObjectNode) dataNode).putArray(Constants.SEARCHTAGS).add(searchTagsArray);
                dataNode = addExtraFields(dataNode);
                if(eachCompTheme.has(Constants.COMPETENCY_TYPE) && !eachCompTheme.get(
                    Constants.COMPETENCY_TYPE).asText().isEmpty()){
                  JsonNode addtionalProperty = objectMapper.createObjectNode();
                  ((ObjectNode) addtionalProperty).put(Constants.THEME_TYPE, eachCompTheme.get(
                      Constants.COMPETENCY_TYPE).asText());
                  ((ObjectNode) dataNode).put(Constants.ADDITIONAL_PROPERTIES, addtionalProperty);
                }
                compThemeEntity.setId(formattedId);
                compThemeEntity.setData(dataNode);
                compThemeEntity.setIsActive(true);
                compThemeEntity.setCreatedOn(currentTime);
                compThemeEntity.setUpdatedOn(currentTime);
                competencyThemeRepository.save(compThemeEntity);
                log.info(
                    "CompetencyThemeService::loadCompetencyThemeFromExcel::persited compTheme in postgres with id: "
                        + formattedId);
                Map<String, Object> map = objectMapper.convertValue(dataNode, Map.class);
                esUtilService.addDocument(Constants.COMP_THEME_INDEX_NAME, Constants.INDEX_TYPE,
                    formattedId, map, cbServerProperties.getElasticCompJsonPath());
                cacheService.putCache(formattedId, dataNode);
                log.info(
                    "CompetencyThemeService::loadCompetencyThemeExcel::created the compTheme with: "
                        + formattedId);
              }
            }

          });
    }

  }

  @Override
  public CustomResponse searchCompTheme(SearchCriteria searchCriteria) {
    log.info("CompetencyThemeService::searchCompTheme");
    CustomResponse response = new CustomResponse();
    SearchResult searchResult = redisTemplate.opsForValue()
        .get(generateRedisJwtTokenKey(searchCriteria));
    if (searchResult != null) {
      log.info("searchCompTheme:search result fetched from redis");
      response.getResult().put(Constants.RESULT, searchResult);
      createSuccessResponse(response);
      return response;
    }
    String searchString = searchCriteria.getSearchString();
    if (searchString != null && searchString.length() < 2) {
      createErrorResponse(response, "Minimum 3 characters are required to search",
          HttpStatus.BAD_REQUEST,
          Constants.FAILED_CONST);
      return response;
    }
    try {
      searchResult =
          esUtilService.searchDocuments(Constants.COMP_THEME_INDEX_NAME, searchCriteria);
      response.getResult().put(Constants.RESULT, searchResult);
      createSuccessResponse(response);
      return response;
    } catch (Exception e) {
      createErrorResponse(response, e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR,
          Constants.FAILED_CONST);
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

  public void createSuccessResponse(CustomResponse response) {
    response.setParams(new RespParam());
    response.getParams().setStatus(Constants.SUCCESS);
    response.setResponseCode(HttpStatus.OK);
  }

  public void createErrorResponse(
      CustomResponse response, String errorMessage, HttpStatus httpStatus, String status) {
    response.setParams(new RespParam());
    response.getParams().setStatus(status);
    response.setResponseCode(httpStatus);
  }

  private JsonNode addExtraFields(JsonNode jsonNode) {
    log.info("CompetencyThemeService::addExtraFields");
    ((ObjectNode) jsonNode).put(Constants.TYPE, Constants.COMPETENCY_THEME_TYPE);
    ((ObjectNode) jsonNode).put(Constants.VERSION, 1);
    ((ObjectNode) jsonNode).put(Constants.SOURCE, (JsonNode) null);
    ((ObjectNode) jsonNode).putArray(Constants.ADDITIONAL_PROPERTIES);
    ((ObjectNode) jsonNode).put(Constants.LEVEL, Constants.INITIATIVE);
    ((ObjectNode) jsonNode).put(Constants.IS_ACTIVE, true);
    ((ObjectNode) jsonNode).put(Constants.LEVEL_ID, 0);
    ((ObjectNode) jsonNode).put(Constants.REVIEWED_BY, (JsonNode) null);
    ((ObjectNode) jsonNode).put(Constants.REVIEWED_DATE, (JsonNode) null);
    ((ObjectNode) jsonNode).put(Constants.UPDATED_BY, (JsonNode) null);
    ((ObjectNode) jsonNode).put(Constants.ADDITIONAL_PROPERTIES, (JsonNode) null);
    return jsonNode;
  }
}

package com.igot.cb.competencies.area.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.competencies.area.entity.CompetencyAreaEntity;
import com.igot.cb.competencies.area.repository.CompetencyAreaRepository;
import com.igot.cb.competencies.area.service.CompetencyAreaService;
import com.igot.cb.designation.entity.DesignationEntity;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.FileProcessService;
import com.igot.cb.pores.util.PayloadValidation;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class CompetencyAreaServiceImpl implements CompetencyAreaService {

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
  private CompetencyAreaRepository competencyAreaRepository;

  @Autowired
  private FileProcessService fileProcessService;


  @Override
  public void loadCompetencyArea(MultipartFile file) {
    log.info("DesignationServiceImpl::loadDesignationFromExcel");
    List<Map<String, String>> processedData = fileProcessService.processExcelFile(file);
    log.info("No.of processedData from excel: " + processedData.size());
    JsonNode designationJson = objectMapper.valueToTree(processedData);
    AtomicLong startingId = new AtomicLong(competencyAreaRepository.count());
    CompetencyAreaEntity competencyAreaEntity = new CompetencyAreaEntity();
    designationJson.forEach(
        eachDesignation -> {
          String formattedId = String.format("DESG-%06d", startingId.incrementAndGet());
          if (!eachDesignation.isNull()) {
            ((ObjectNode) eachDesignation).put(Constants.ID, formattedId);
            if (eachDesignation.has(Constants.UPDATED_DESIGNATION) && !eachDesignation.get(
                Constants.UPDATED_DESIGNATION).isNull()) {
              ((ObjectNode) eachDesignation).put(Constants.DESIGNATION,
                  eachDesignation.get(Constants.UPDATED_DESIGNATION));
            }
            String descriptionValue =
                (eachDesignation.has(Constants.DESCRIPTION_PAYLOAD) && !eachDesignation.get(
                    Constants.DESCRIPTION_PAYLOAD).isNull())
                    ? eachDesignation.get(Constants.UPDATED_DESIGNATION).asText("")
                    : "";
            ((ObjectNode) eachDesignation).put(Constants.DESCRIPTION, descriptionValue);
            payloadValidation.validatePayload(Constants.DESIGNATION_PAYLOAD_VALIDATION,
                eachDesignation);
            ((ObjectNode) eachDesignation).put(Constants.STATUS, Constants.ACTIVE);
            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            ((ObjectNode) eachDesignation).put(Constants.CREATED_ON, String.valueOf(currentTime));
            ((ObjectNode) eachDesignation).put(Constants.UPDATED_ON, String.valueOf(currentTime));
            competencyAreaEntity.setId(formattedId);
            competencyAreaEntity.setData(eachDesignation);
            competencyAreaEntity.setIsActive(true);
            competencyAreaEntity.setCreatedOn(currentTime);
            competencyAreaEntity.setUpdatedOn(currentTime);
            designationRepository.save(competencyAreaEntity);
            log.info(
                "DesignationServiceImpl::loadDesignationFromExcel::persited designation in postgres with id: "
                    + formattedId);
            Map<String, Object> map = objectMapper.convertValue(eachDesignation, Map.class);
            esUtilService.addDocument(Constants.DESIGNATION_INDEX_NAME, Constants.INDEX_TYPE,
                formattedId, map, cbServerProperties.getElasticDesignationJsonPath());
            cacheService.putCache(formattedId, eachDesignation);
            log.info(
                "DesignationServiceImpl::loadDesignationFromExcel::created the designation with: "
                    + formattedId);
          }

        });
  }
}

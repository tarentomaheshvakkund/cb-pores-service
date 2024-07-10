package com.igot.cb.designation.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.designation.entity.DesignationEntity;
import com.igot.cb.designation.repository.DesignationRepository;
import com.igot.cb.designation.service.DesignationService;
import com.igot.cb.interest.entity.Interests;
import com.igot.cb.interest.service.impl.InterestServiceImpl;
import com.igot.cb.pores.cache.CacheService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.service.EsUtilService;
import com.igot.cb.pores.exceptions.CustomException;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.pores.util.PayloadValidation;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class DesignationServiceImpl implements DesignationService {

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  private DesignationRepository designationRepository;

  @Autowired
  private PayloadValidation payloadValidation;

  @Autowired
  private EsUtilService esUtilService;

  @Autowired
  private CacheService cacheService;

  @Autowired
  private CbServerProperties cbServerProperties;

  private Logger logger = LoggerFactory.getLogger(InterestServiceImpl.class);

  @Override
  public void loadDesignationFromExcel(MultipartFile file) {
    log.info("DesignationServiceImpl::loadDesignationFromExcel");
    List<Map<String, String>> processedData = processExcelFile(file);
    log.info("No.of processedData from excel: " + processedData.size());
    JsonNode designationJson = objectMapper.valueToTree(processedData);
    AtomicLong startingId = new AtomicLong(designationRepository.count());
    DesignationEntity designationEntity = new DesignationEntity();
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
            designationEntity.setId(formattedId);
            designationEntity.setData(eachDesignation);
            designationEntity.setIsActive(true);
            designationEntity.setCreatedOn(currentTime);
            designationEntity.setUpdatedOn(currentTime);
            designationRepository.save(designationEntity);
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
    log.info("DesignationServiceImpl::loadDesignationFromExcel::created the designations");
  }

  @Override
  public CustomResponse readDesignation(String id) {
    log.info("DesignationServiceImpl::readDesignation::reading the designation");
    CustomResponse response = new CustomResponse();
    if (StringUtils.isEmpty(id)) {
      logger.error("InterestServiceImpl::read:Id not found");
      response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
      response.setMessage(Constants.ID_NOT_FOUND);
      return response;
    }
    try {
      String cachedJson = cacheService.getCache(id);
      if (StringUtils.isNotEmpty(cachedJson)) {
        log.info("InterestServiceImpl::read:Record coming from redis cache");
        response.setMessage(Constants.SUCCESSFULLY_READING);
        response
            .getResult()
            .put(Constants.RESULT, objectMapper.readValue(cachedJson, new TypeReference<Object>() {
            }));
      } else {
        Optional<DesignationEntity> entityOptional = designationRepository.findByIdAndIsActive(id, true);
        if (entityOptional.isPresent()) {
          DesignationEntity designationEntity = entityOptional.get();
          cacheService.putCache(id, designationEntity.getData());
          log.info("InterestServiceImpl::read:Record coming from postgres db");
          response.setMessage(Constants.SUCCESSFULLY_READING);
          response
              .getResult()
              .put(Constants.RESULT,
                  objectMapper.convertValue(
                      designationEntity.getData(), new TypeReference<Object>() {
                      }));
          response.setResponseCode(HttpStatus.OK);
        } else {
          logger.error("Invalid Id: {}", id);
          response.setResponseCode(HttpStatus.NOT_FOUND);
          response.setMessage(Constants.INVALID_ID);
        }
      }
    } catch (Exception e) {
      logger.error("Error while mapping JSON for id {}: {}", id, e.getMessage(), e);
      throw new CustomException(Constants.ERROR, "error while processing",
          HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return response;
  }

  private List<Map<String, String>> processExcelFile(MultipartFile incomingFile) {
    log.info("DesignationServiceImpl::processExcelFile");
    try {
      return validateFileAndProcessRows(incomingFile);
    } catch (Exception e) {
      log.error("Error occurred during file processing: {}", e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  private List<Map<String, String>> validateFileAndProcessRows(MultipartFile file) {
    log.info("DesignationServiceImpl::validateFileAndProcessRows");
    log.info("DesignationServiceImpl::validateFileAndProcessRows");
    String fileName = file.getOriginalFilename();
    if (fileName == null) {
      throw new RuntimeException("File name is null");
    }

    try (InputStream inputStream = file.getInputStream()) {
      if (fileName.endsWith(".xlsx") || fileName.endsWith(".xls")) {
        Workbook workbook = WorkbookFactory.create(inputStream);
        Sheet sheet = workbook.getSheetAt(0);
        return processSheetAndSendMessage(sheet);
      } else if (fileName.endsWith(".csv")) {
        return processCsvAndSendMessage(inputStream);
      } else {
        throw new RuntimeException("Unsupported file type: " + fileName);
      }
    } catch (IOException e) {
      log.error("Error while processing file: {}", e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  private List<Map<String, String>> processSheetAndSendMessage(Sheet sheet) {
    log.info("DesignationServiceImpl::processSheetAndSendMessage");
    try {
      DataFormatter formatter = new DataFormatter();
      Row headerRow = sheet.getRow(0);
      List<Map<String, String>> dataRows = new ArrayList<>();
      for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
        Row dataRow = sheet.getRow(rowIndex);
        if (dataRow == null) {
          break; // No more data rows, exit the loop
        }
        boolean allBlank = true;
        Map<String, String> rowData = new HashMap<>();
        for (int colIndex = 0; colIndex < headerRow.getLastCellNum(); colIndex++) {
          Cell headerCell = headerRow.getCell(colIndex);
          Cell valueCell = dataRow.getCell(colIndex);
          if (headerCell != null && headerCell.getCellType() != CellType.BLANK) {
            String excelHeader =
                formatter.formatCellValue(headerCell).replaceAll("[\\n*]", "").trim();
            String cellValue = "";
            if (valueCell != null && valueCell.getCellType() != CellType.BLANK) {
              if (valueCell.getCellType() == CellType.NUMERIC
                  && DateUtil.isCellDateFormatted(valueCell)) {
                // Handle date format
                Date date = valueCell.getDateCellValue();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                cellValue = dateFormat.format(date);
              } else {
                cellValue = formatter.formatCellValue(valueCell).replace("\n", ",").trim();
              }
              allBlank = false;
            }
            rowData.put(excelHeader, cellValue);
          }
        }
        log.info("Data Rows: " + rowData);
        if (allBlank) {
          break; // If all cells are blank in the current row, stop processing
        }
        dataRows.add(rowData);
      }
      log.info("Number of Data Rows Processed: " + dataRows.size());
      return dataRows;
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  private List<Map<String, String>> processCsvAndSendMessage(InputStream inputStream) throws IOException {
    log.info("DesignationServiceImpl::processCsvAndSendMessage");
    List<Map<String, String>> dataRows = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

      List<String> headers = csvParser.getHeaderNames();

      for (CSVRecord csvRecord : csvParser) {
        boolean allBlank = true;
        Map<String, String> rowData = new HashMap<>();
        for (String header : headers) {
          String cellValue = csvRecord.get(header);
          if (cellValue != null && !cellValue.trim().isEmpty()) {
            // Handle date format (assuming date is in a specific format)
            if (isDate(cellValue)) {
              SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
              cellValue = dateFormat.format(parseDate(cellValue));
            } else {
              cellValue = cellValue.replace("\n", ",").trim();
            }
            allBlank = false;
          }
          rowData.put(header, cellValue);
        }
        log.info("Data Rows: " + rowData);
        if (allBlank) {
          break; // If all cells are blank in the current row, stop processing
        }
        dataRows.add(rowData);
      }
      log.info("Number of Data Rows Processed: " + dataRows.size());
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
    return dataRows;
  }

  private boolean isDate(String value) {
    try {
      parseDate(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private Date parseDate(String value) throws Exception {
    // Customize this date parsing logic based on the expected date format in your CSV
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    return dateFormat.parse(value);
  }

}

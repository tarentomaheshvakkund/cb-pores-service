package com.igot.cb.competencies.area.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.competencies.area.service.CompetencyAreaService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/competencyArea")
@Slf4j
public class CompetencyAreaController {

  @Autowired
  private CompetencyAreaService competencyAreaService;

  @PostMapping(value = "/upload", consumes = "multipart/form-data")
  public ResponseEntity<String> loadCompetencyAreas(@RequestParam("file") MultipartFile file, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    try {
      competencyAreaService.loadCompetencyArea(file, token);
      return ResponseEntity.ok("Loading of designations from excel is successful.");
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("Error during loading of designation from excel: " + e.getMessage());
    }
  }

  @PostMapping("/create")
  public ResponseEntity<CustomResponse> createCompetencyArea(@RequestBody JsonNode competencyArea) {
    CustomResponse response = competencyAreaService.createCompArea(competencyArea);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PutMapping(value = "/update", produces = "application/json")
  public ResponseEntity<CustomResponse> update(@RequestBody JsonNode updatedCompArea) {
    CustomResponse response = competencyAreaService.updateCompArea(updatedCompArea);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PostMapping("/search")
  public ResponseEntity<?> search(@RequestBody SearchCriteria searchCriteria) {
    CustomResponse response = competencyAreaService.searchCompArea(searchCriteria);
    return new ResponseEntity<>(response, response.getResponseCode());
  }
}

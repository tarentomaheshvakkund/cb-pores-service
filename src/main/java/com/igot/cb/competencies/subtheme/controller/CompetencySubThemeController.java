package com.igot.cb.competencies.subtheme.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.competencies.subtheme.service.CompetencySubThemeService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


@RestController
@RequestMapping("/competencySubTheme")
@Slf4j
public class CompetencySubThemeController {

  @Autowired
  private CompetencySubThemeService competencySubThemeService;

  @PostMapping(value = "/upload", consumes = "multipart/form-data")
  public ResponseEntity<String> loadCompetencyAreas(@RequestParam("file") MultipartFile file, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    try {
      competencySubThemeService.loadCompetencySubTheme(file, token);
      return ResponseEntity.ok("Loading of competencySubTheme from excel is successful.");
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("Error during loading of competencySubTheme from excel: " + e.getMessage());
    }
  }

  @PostMapping("/search")
  public ResponseEntity<?> search(@RequestBody SearchCriteria searchCriteria) {
    CustomResponse response = competencySubThemeService.searchCompSubTheme(searchCriteria);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PostMapping("/create")
  public ResponseEntity<CustomResponse> createCompetencySubTheme(@RequestBody JsonNode competencySubTheme, @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
    CustomResponse response = competencySubThemeService.createCompSubTheme(competencySubTheme, token);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PutMapping(value = "/update", produces = "application/json")
  public ResponseEntity<CustomResponse> update(@RequestBody JsonNode updatedCompSubTheme) {
    CustomResponse response = competencySubThemeService.updateCompSubTheme(updatedCompSubTheme);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/read/{id}")
  public ResponseEntity<?> competencySubThemeRead(@PathVariable String id) {
    CustomResponse response = competencySubThemeService.readCompSubTheme(id);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @DeleteMapping("/delete/{id}")
  public ResponseEntity<CustomResponse> deleteCompetencySubTheme(@PathVariable String id) {
    CustomResponse response = competencySubThemeService.deleteCompetencySubTheme(id);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

}

package com.igot.cb.competencies.area.controller;

import com.igot.cb.competencies.area.service.CompetencyAreaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
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
  public ResponseEntity<String> loadCompetencyAreas(@RequestParam("file") MultipartFile file) {
    try {
      competencyAreaService.loadCompetencyArea(file);
      return ResponseEntity.ok("Loading of designations from excel is successful.");
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("Error during loading of designation from excel: " + e.getMessage());
    }
  }
}

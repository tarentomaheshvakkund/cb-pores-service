package com.igot.cb.designation.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.designation.service.DesignationService;
import com.igot.cb.pores.util.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@RestController
@RequestMapping("/designation")
@Slf4j
public class DesignationController {

  @Autowired
  private DesignationService designationService;

  @PostMapping(value = "/upload", consumes = "multipart/form-data")
  public ResponseEntity<String> loadJobsFromExcel(@RequestParam("file") MultipartFile file) {
    try {
      designationService.loadDesignationFromExcel(file);
      return ResponseEntity.ok("Loading of designations from excel is successful.");
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("Error during loading of designation from excel: " + e.getMessage());
    }
  }

  @PostMapping("/create")
  public ResponseEntity<ApiResponse> createDesignation(@RequestBody JsonNode request) {
    ApiResponse response = designationService.createDesignation(request);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

}

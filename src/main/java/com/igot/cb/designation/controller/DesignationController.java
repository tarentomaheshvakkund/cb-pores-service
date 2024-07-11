package com.igot.cb.designation.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.designation.service.DesignationService;
import com.igot.cb.pores.dto.CustomResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/designation")
@Slf4j
public class DesignationController {

  @Autowired
  private DesignationService designationService;

  @PostMapping(value = "/upload", consumes = "multipart/form-data")
  public ResponseEntity<String> loadDesignation(@RequestParam("file") MultipartFile file) {
    try {
      designationService.loadDesignation(file);
      return ResponseEntity.ok("Loading of designations from excel is successful.");
    } catch (Exception e) {
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body("Error during loading of designation from excel: " + e.getMessage());
    }
  }

  @GetMapping("/read/{id}")
  public ResponseEntity<?> playListRead(@PathVariable String id) {
    CustomResponse response = designationService.readDesignation(id);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  //update API to store the refNodes

  @PutMapping(value = "/update", produces = "application/json")
  public ResponseEntity<CustomResponse> assign(@RequestBody JsonNode updateDesignationDetails) {
    CustomResponse response = designationService.updateDesignation(updateDesignationDetails);
    return new ResponseEntity<>(response, response.getResponseCode());
  }


}

package com.igot.cb.announcement.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.announcement.service.AnnouncementService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/announcement")
public class AnnouncementController {

  @Autowired
  private AnnouncementService announcementService;

  @PostMapping("/v1/create")
  public ResponseEntity<CustomResponse> create(@RequestBody JsonNode announcementEntity) {
    CustomResponse response = announcementService.createAnnouncement(announcementEntity);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PostMapping("/v1/search")
  public ResponseEntity<?> search(@RequestBody SearchCriteria searchCriteria) {
    CustomResponse response = announcementService.searchAnnouncement(searchCriteria);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PutMapping("/v1/update")
  public ResponseEntity<CustomResponse> assign(@RequestBody JsonNode interestDetails) {
    CustomResponse response = announcementService.updateAnnouncement(interestDetails);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/v1/read/{id}")
  public ResponseEntity<?> read(@PathVariable String id) {
    CustomResponse response = announcementService.readAnnouncement(id);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @DeleteMapping("/v1/delete/{id}")
  public ResponseEntity<?> delete(@PathVariable String id) {
    CustomResponse response = announcementService.deleteAnnouncement(id);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
}

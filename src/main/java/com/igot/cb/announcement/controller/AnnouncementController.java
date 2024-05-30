package com.igot.cb.announcement.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.announcement.service.AnnouncementService;
import com.igot.cb.pores.dto.CustomResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
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
}

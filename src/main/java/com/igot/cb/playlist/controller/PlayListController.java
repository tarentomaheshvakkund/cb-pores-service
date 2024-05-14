package com.igot.cb.playlist.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.playlist.service.PlayListSerive;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.util.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/playList")
public class PlayListController {
  @Autowired
  private PlayListSerive playListSerive;

  @PostMapping("/create")
  public Object create(@RequestBody JsonNode playListDetails) {
    ApiResponse response = playListSerive.createPlayList(playListDetails);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/read/{orgId}")
  public Object read(@PathVariable String orgId) throws JsonProcessingException {
    ApiResponse response = playListSerive.readPlayList(orgId);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
  @PutMapping("/update")
  public Object update(@RequestBody JsonNode playListDetails)
      throws JsonProcessingException {
    ApiResponse response = playListSerive.updatePlayList(playListDetails);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

}

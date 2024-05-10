package com.igot.cb.playlist.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.playlist.service.PlayListSerive;
import com.igot.cb.pores.dto.CustomResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/playList")
public class PlayListController {
  @Autowired
  private PlayListSerive playListSerive;

  @PostMapping("/create")
  public ResponseEntity<CustomResponse> create(@RequestBody JsonNode playListDetails) {
    CustomResponse response = playListSerive.createPlayList(playListDetails);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/read/{playListId}")
  public ResponseEntity<?> read(@PathVariable String playListId) {
    CustomResponse response = playListSerive.readPlayList(playListId);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

}

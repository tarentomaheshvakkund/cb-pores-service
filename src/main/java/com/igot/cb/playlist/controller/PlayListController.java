package com.igot.cb.playlist.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.playlist.dto.SearchDto;
import com.igot.cb.playlist.service.PlayListSerive;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
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

  @PostMapping("/search")
  public Object playListRead(@RequestBody SearchDto searchDto) {
    ApiResponse response = playListSerive.searchPlayListForOrg(searchDto);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PutMapping("/update")
  public Object update(@RequestBody JsonNode playListDetails)
      throws JsonProcessingException {
    ApiResponse response = playListSerive.updatePlayList(playListDetails);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @DeleteMapping("/delete/{id}")
  public Object delete(@PathVariable String id) {
    ApiResponse response = playListSerive.delete(id);
    return new ResponseEntity<>(response, response.getResponseCode());
  }
  @PostMapping("/v2/search")
  public Object playListSearch(@RequestBody SearchCriteria searchDto) {
    ApiResponse response = playListSerive.searchPlayList(searchDto);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @PostMapping("/read/{id}/{orgId}")
  public Object playListRead(@PathVariable String id, @PathVariable String orgId) {
    ApiResponse response = playListSerive.readPlaylist(id, orgId);
    return new ResponseEntity<>(response, response.getResponseCode());
  }
}

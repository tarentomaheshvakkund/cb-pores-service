package com.igot.cb.playlist.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.util.ApiResponse;

public interface PlayListSerive {

  ApiResponse createPlayList(JsonNode playListDetails);

  ApiResponse readPlayList(String orgId) throws JsonProcessingException;

  ApiResponse updatePlayList(JsonNode playListDetails) throws JsonProcessingException;
}

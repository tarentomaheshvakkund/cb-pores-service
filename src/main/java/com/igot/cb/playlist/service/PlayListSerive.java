package com.igot.cb.playlist.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;

public interface PlayListSerive {

  CustomResponse createPlayList(JsonNode playListDetails);

  CustomResponse readPlayList(String playListId);
}

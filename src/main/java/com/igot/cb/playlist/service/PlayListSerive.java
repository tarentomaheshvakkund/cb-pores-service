package com.igot.cb.playlist.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.playlist.dto.SearchDto;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.ApiResponse;

public interface PlayListSerive {

  ApiResponse createPlayList(JsonNode playListDetails);

  ApiResponse searchPlayListForOrg(SearchDto searchDto);

  ApiResponse updatePlayList(JsonNode playListDetails);

  ApiResponse delete(String id);

  ApiResponse searchPlayList(SearchCriteria searchCriteria);

  ApiResponse readPlaylist(String id, String orgId);
}

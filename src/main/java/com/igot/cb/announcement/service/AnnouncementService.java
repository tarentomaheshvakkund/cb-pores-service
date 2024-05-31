package com.igot.cb.announcement.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;

public interface AnnouncementService {

  CustomResponse createAnnouncement(JsonNode announcementEntity);

  CustomResponse searchAnnouncement(SearchCriteria searchCriteria);

  CustomResponse updateAnnouncement(JsonNode interestDetails);

  CustomResponse readAnnouncement(String id);

  CustomResponse deleteAnnouncement(String id);
}

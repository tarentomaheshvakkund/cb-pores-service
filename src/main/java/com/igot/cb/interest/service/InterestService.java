package com.igot.cb.interest.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;

public interface InterestService {

  CustomResponse createInterest(JsonNode announcementdetails);

  CustomResponse searchDemand(SearchCriteria searchCriteria);

  CustomResponse assignInterestToDemand(JsonNode announcementDetails);

  CustomResponse read(String id);
}

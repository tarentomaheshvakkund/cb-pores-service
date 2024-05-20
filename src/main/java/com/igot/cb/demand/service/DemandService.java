package com.igot.cb.demand.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;

public interface DemandService {
  CustomResponse createDemand(JsonNode demandDetails);

  CustomResponse readDemand(String id);

  CustomResponse searchDemand(SearchCriteria searchCriteria);

  String delete(String id);

  CustomResponse updateDemand(JsonNode demandsDetails);
}

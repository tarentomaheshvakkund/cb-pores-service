package com.igot.cb.competencies.area.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import org.springframework.stereotype.Repository;
import org.springframework.web.multipart.MultipartFile;

@Repository
public interface  CompetencyAreaService {

  void loadCompetencyArea(MultipartFile file, String token);

 public CustomResponse createCompArea(JsonNode competencyArea);

  public CustomResponse updateCompArea(JsonNode updatedCompArea);

  public CustomResponse searchCompArea(SearchCriteria searchCriteria);
}

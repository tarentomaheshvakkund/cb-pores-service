package com.igot.cb.competencies.theme.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import org.springframework.web.multipart.MultipartFile;

public interface CompetencyThemeService {

  public void loadCompetencyTheme(MultipartFile file, String token);

 public CustomResponse searchCompTheme(SearchCriteria searchCriteria);

 public CustomResponse createCompTheme(JsonNode competencyArea, String token);

 public CustomResponse updateCompTheme(JsonNode updatedCompArea);

 public CustomResponse readCompTheme(String id);

 public CustomResponse deleteCompetencyTheme(String id);
}

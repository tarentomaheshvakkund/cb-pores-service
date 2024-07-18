package com.igot.cb.competencies.subtheme.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.ApiResponse;
import org.springframework.web.multipart.MultipartFile;

public interface CompetencySubThemeService {

  public void loadCompetencySubTheme(MultipartFile file, String token);

  public CustomResponse searchCompSubTheme(SearchCriteria searchCriteria);

  public CustomResponse createCompSubTheme(JsonNode competencySubTheme, String token);

  public CustomResponse updateCompSubTheme(JsonNode updatedCompSubTheme);

  public CustomResponse readCompSubTheme(String id);

  public CustomResponse deleteCompetencySubTheme(String id);

  public ApiResponse createTerm(JsonNode request);
}

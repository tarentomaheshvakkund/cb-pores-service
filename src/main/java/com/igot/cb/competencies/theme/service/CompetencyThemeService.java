package com.igot.cb.competencies.theme.service;

import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import org.springframework.web.multipart.MultipartFile;

public interface CompetencyThemeService {

  public void loadCompetencyTheme(MultipartFile file, String token);

 public CustomResponse searchCompTheme(SearchCriteria searchCriteria);
}

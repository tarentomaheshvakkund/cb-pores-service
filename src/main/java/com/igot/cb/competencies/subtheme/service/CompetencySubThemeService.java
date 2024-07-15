package com.igot.cb.competencies.subtheme.service;

import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import org.springframework.web.multipart.MultipartFile;

public interface CompetencySubThemeService {

  public void loadCompetencySubTheme(MultipartFile file, String token);

  public CustomResponse searchCompSubTheme(SearchCriteria searchCriteria);
}

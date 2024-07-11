package com.igot.cb.competencies.area.service;

import org.springframework.stereotype.Repository;
import org.springframework.web.multipart.MultipartFile;

@Repository
public interface  CompetencyAreaService {

  void loadCompetencyArea(MultipartFile file);
}

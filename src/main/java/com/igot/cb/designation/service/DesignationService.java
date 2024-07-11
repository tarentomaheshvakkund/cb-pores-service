package com.igot.cb.designation.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import org.springframework.web.multipart.MultipartFile;

public interface DesignationService {

  public void loadDesignation(MultipartFile file);

  public CustomResponse readDesignation(String id);

  public CustomResponse updateDesignation(JsonNode updateDesignationDetails);
}

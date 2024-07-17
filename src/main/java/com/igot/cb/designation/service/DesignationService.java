package com.igot.cb.designation.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.util.ApiResponse;
import org.springframework.web.multipart.MultipartFile;

public interface DesignationService {

 public void loadDesignationFromExcel(MultipartFile file);

 public ApiResponse createTerm(JsonNode request);

 public   CustomResponse updateIdentifiersToDesignation(JsonNode updateDesignationDetails);
}

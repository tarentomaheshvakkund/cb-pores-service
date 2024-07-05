package com.igot.cb.designation.service;

import org.springframework.web.multipart.MultipartFile;

public interface DesignationService {

 public void loadDesignationFromExcel(MultipartFile file);
}

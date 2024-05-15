package com.igot.cb.playlist.service;

import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;


public interface ContentService {

  public Map<String, Object> readContentFromCache(String contentId, List<String> fields);

  public Map<String, Object> readContent(String contentId);

  public Map<String, Object> readContent(String contentId, List<String> fields);

}

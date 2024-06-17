package com.igot.cb.cios.service;


import com.igot.cb.cios.dto.ObjectDto;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface CiosContentService {
    SearchResult searchCotent(SearchCriteria searchCriteria);

    Object fetchDataByContentId(String contentId);

    Object deleteContent(String contentId);

    Object onboardContent(List<ObjectDto> data);



}

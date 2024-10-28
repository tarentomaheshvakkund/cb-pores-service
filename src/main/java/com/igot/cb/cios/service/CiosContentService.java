package com.igot.cb.cios.service;


import com.igot.cb.cios.dto.ObjectDto;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import com.igot.cb.pores.util.ApiResponse;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface CiosContentService {
    SearchResult searchCotent(SearchCriteria searchCriteria);

    Object fetchDataByContentId(String contentId);

    Object deleteContent(String contentId);

    ApiResponse onboardContent(List<ObjectDto> data);

    Object fetchDataByExternalIdAndPartnerId(String externalid,String partnerid);

}

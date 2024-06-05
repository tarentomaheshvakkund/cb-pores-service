package com.igot.cb.orgbookmark.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.ApiResponse;
import org.springframework.stereotype.Service;

@Service
public interface OrgBookmarkService {
    ApiResponse createOrgBookmark(JsonNode orgDetails, String userAuthToken);

    ApiResponse updateOrgBookmark(JsonNode orgDetails, String userAuthToken);

    ApiResponse readOrgBookmarkById(String id);

    CustomResponse search(SearchCriteria searchCriteria);

    ApiResponse deleteOrgBookmarkById(String id);

}

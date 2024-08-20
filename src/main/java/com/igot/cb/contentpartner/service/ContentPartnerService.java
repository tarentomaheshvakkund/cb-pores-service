package com.igot.cb.contentpartner.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.contentpartner.entity.ContentPartnerEntity;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.ApiResponse;

public interface ContentPartnerService {
    ApiResponse createOrUpdate(JsonNode demandsJson);

    ApiResponse read(String id);

    ApiResponse searchEntity(SearchCriteria searchCriteria);

    ApiResponse delete(String id);

    ApiResponse getContentDetailsByPartnerName(String name);

}

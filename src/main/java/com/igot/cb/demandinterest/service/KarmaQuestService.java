package com.igot.cb.demandinterest.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;

/**
 * @author Mahesh RV
 * @author Ruksana
 */
public interface KarmaQuestService {

    Object getInterest(String interestId);

    Object insertInterest(JsonNode requestBodyMap);
    CustomResponse updateDemand(JsonNode demandsDetails);
}

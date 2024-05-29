package com.igot.cb.demand.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.demand.service.DemandService;
import com.igot.cb.pores.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/demand")
public class DemandController {
  @Autowired
  private DemandService demandService;

  @PostMapping("/create")
  public ResponseEntity<CustomResponse> create(@RequestBody JsonNode demandsDetails,
                                                @RequestHeader(Constants.X_AUTH_TOKEN) String token,
                                                @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId) {
    CustomResponse response = demandService.createDemand(demandsDetails,token, rootOrgId);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @GetMapping("/read/{id}")
  public ResponseEntity<?> read(@PathVariable String id) {
    CustomResponse response = demandService.readDemand(id);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }
  @PostMapping("/search")
  public ResponseEntity<?> search(@RequestBody SearchCriteria searchCriteria) {
    CustomResponse response = demandService.searchDemand(searchCriteria);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

  @DeleteMapping("/delete/{id}")
  public ResponseEntity<String> delete(@PathVariable String id) {
    String response = demandService.delete(id);
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @PostMapping("/v1/update/status")
  public ResponseEntity<CustomResponse> updateStatus(@RequestBody JsonNode updateDetails,
                                                     @RequestHeader(Constants.X_AUTH_TOKEN) String token,
                                                     @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId) {
    CustomResponse response = demandService.updateDemandStatus(updateDetails, token,rootOrgId);
    return new ResponseEntity<>(response, response.getResponseCode());
  }

}

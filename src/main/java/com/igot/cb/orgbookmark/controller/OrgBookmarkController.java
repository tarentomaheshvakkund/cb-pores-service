package com.igot.cb.orgbookmark.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.orgbookmark.service.OrgBookmarkService;
import com.igot.cb.pores.dto.CustomResponse;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orgBookmark")
public class OrgBookmarkController {
    @Autowired
    private OrgBookmarkService orgBookmarkService;

    @PostMapping("/v1/create")
    public ResponseEntity<?> createOrgBookmark(@RequestBody JsonNode orgDetails,
                                               @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {
        ApiResponse response = orgBookmarkService.createOrgBookmark(orgDetails, userAuthToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/v1/update")
    public ResponseEntity<?> updateOrgBookmark(@RequestBody JsonNode orgDetails,
                                               @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {
        ApiResponse response = orgBookmarkService.updateOrgBookmark(orgDetails, userAuthToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/read/{id}")
    public ResponseEntity<?> readOrgBookmarkById(@PathVariable String id) {
        ApiResponse response = orgBookmarkService.readOrgBookmarkById(id);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/v1/search")
    public ResponseEntity<?> search(@RequestBody SearchCriteria searchCriteria) {
        CustomResponse response = orgBookmarkService.search(searchCriteria);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @DeleteMapping("/v1/delete/{id}")
    public ResponseEntity<?> deleteOrgBookmarkById(@PathVariable String id,
                                                   @RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        ApiResponse response = orgBookmarkService.deleteOrgBookmarkById(id);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}

package com.igot.cb.cios.controller;



import com.igot.cb.cios.dto.ObjectDto;
import com.igot.cb.cios.service.CiosContentService;
import com.igot.cb.pores.elasticsearch.dto.SearchCriteria;
import com.igot.cb.pores.elasticsearch.dto.SearchResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/cios")
@Slf4j
public class CiosController {
    @Autowired
    CiosContentService ciosContentService;

    @PostMapping(value = "/v1/onboardContent")
    public ResponseEntity<Object> onboardContent(@RequestBody List<ObjectDto> objectDtos) {

        return new ResponseEntity<>(ciosContentService.onboardContent(objectDtos), HttpStatus.OK);
    }

    @PostMapping(value = "/v1/search/content")
    public ResponseEntity<?> searchContent(@RequestBody SearchCriteria searchCriteria) {
        SearchResult searchResult = ciosContentService.searchCotent(searchCriteria);
        return new ResponseEntity<>(searchResult, HttpStatus.OK);
    }

    @DeleteMapping("/v1/content/delete/{contentId}")
    public ResponseEntity<Object> deleteContent(@PathVariable String contentId) {
        return new ResponseEntity<>(ciosContentService.deleteContent(contentId), HttpStatus.OK);
    }

    @GetMapping("/v1/content/read/{contentId}")
    public ResponseEntity<Object> fetchData(@PathVariable String contentId) {
        return new ResponseEntity<>(ciosContentService.fetchDataByContentId(contentId), HttpStatus.OK);
    }

    @GetMapping("/v1/content/readby/externalid/{externalid}/{partnerid}")
    public ResponseEntity<Object> fetchDataByExternalId(@PathVariable String externalid,@PathVariable String partnerid) {
        return new ResponseEntity<>(ciosContentService.fetchDataByExternalIdAndPartnerId(externalid,partnerid), HttpStatus.OK);
    }
}

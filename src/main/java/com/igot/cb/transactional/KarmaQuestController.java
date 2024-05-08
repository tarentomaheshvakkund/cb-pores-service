package com.igot.cb.transactional;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.demandinterest.service.KarmaQuestServiceImpl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Mahesh RV
 * @author Ruksana
 */
@RestController
@RequestMapping("/karmaquests/interest")
public class KarmaQuestController {
    @Autowired
    KarmaQuestServiceImpl serviceClass;

    @GetMapping("/get/{interestId}")
    public Object processInterests(@PathVariable String interestId) {
        return serviceClass.getInterest(interestId);
    }

    @PostMapping("/create")
    public Object createInterest(@RequestBody JsonNode requestBodyMap) {
        return serviceClass.insertInterest(requestBodyMap);
    }

}

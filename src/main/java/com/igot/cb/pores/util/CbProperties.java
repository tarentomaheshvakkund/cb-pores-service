package com.igot.cb.pores.util;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
@Component

public class CbProperties {


    @Value("${odcs.framework.name}")
    private String odcsFrameworkName;

    @Value("${odcs.category.name}")
    private String odcsCategoryName;

    @Value("${knowledge.mv.service}")
    private String knowledgeMS;

    @Value("${odcs.term.create}")
    private String odcsTermCrete;

    @Value("${odcs.category.fields}")
    private String odcsFields;

    public String getOdcsFrameworkName() {
        return odcsFrameworkName;
    }

    public String getOdcsCategoryName() {
        return odcsCategoryName;
    }

    public String getKnowledgeMS() {
        return knowledgeMS;
    }

    public String getOdcsTermCrete() {
        return odcsTermCrete;
    }

    public List<String> getOdcsFields() {
        return Arrays.asList(odcsFields.split(",", -1));
    }
}

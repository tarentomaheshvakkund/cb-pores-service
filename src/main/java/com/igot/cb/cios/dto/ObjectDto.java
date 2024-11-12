package com.igot.cb.cios.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.List;


@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ObjectDto{
    private JsonNode contentData;
    private JsonNode competencies_v5;
    private JsonNode competencies_v6;
    private JsonNode contentPartner;
    private List<String> tags;
    private String status;

}

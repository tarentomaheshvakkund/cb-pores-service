package com.igot.cb.cios.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.igot.cb.cios.dto.ObjectDto;
import com.igot.cb.pores.exceptions.CustomException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class CiosRequestPayloadValidation {

    public void validateModel(ObjectDto objectDto) {

        if (objectDto.getTags() == null) {
            throw new CustomException("TAGS", "Tags are missing in the request", HttpStatus.BAD_REQUEST);
        }

        if (objectDto.getStatus() == null) {
            throw new CustomException("STATUS", "Status is missing in the request", HttpStatus.BAD_REQUEST);
        }
        if (objectDto.getContentData() == null) {
            throw new CustomException("CONTENT_DATA", "Content Data is missing in the request", HttpStatus.BAD_REQUEST);
        }
        if (objectDto.getContentPartner() == null) {
            throw new CustomException("CONTENT_PARTNER", "Content Partner is missing in the request", HttpStatus.BAD_REQUEST);
        }
        if (objectDto.getCompetencies_v5() == null) {
            throw new CustomException("COMPETENCIES_V5", "Competencies are missing in the request", HttpStatus.BAD_REQUEST);
        }
    }
}

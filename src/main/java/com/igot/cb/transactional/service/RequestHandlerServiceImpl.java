package com.igot.cb.transactional.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.igot.cb.demand.service.DemandServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;
@Service
public class RequestHandlerServiceImpl {
    private Logger log = LoggerFactory.getLogger(DemandServiceImpl.class);

    @Autowired
    private RestTemplate restTemplate;

    public Map<String, Object> fetchResultUsingPost(String uri, Object request, Map<String, String> headersValues) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        Map<String, Object> response = null;
        try {
            HttpHeaders headers = new HttpHeaders();
            if (!CollectionUtils.isEmpty(headersValues)) {
                headersValues.forEach((k, v) -> headers.set(k, v));
            }
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Object> entity = new HttpEntity<>(request, headers);
            if (log.isDebugEnabled()) {
                StringBuilder str = new StringBuilder(this.getClass().getCanonicalName()).append(".fetchResult")
                        .append(System.lineSeparator());
                str.append("URI: ").append(uri).append(System.lineSeparator());
                str.append("Request: ").append(mapper.writeValueAsString(request)).append(System.lineSeparator());
                log.debug(str.toString());
            }
            response = restTemplate.postForObject(uri, entity, Map.class);
            if (log.isDebugEnabled()) {
                StringBuilder str = new StringBuilder("Response: ");
                str.append(mapper.writeValueAsString(response)).append(System.lineSeparator());
                log.debug(str.toString());
            }
        } catch (HttpClientErrorException hce) {
            try {
                response = (new ObjectMapper()).readValue(hce.getResponseBodyAsString(),
                        new TypeReference<HashMap<String, Object>>() {
                        });
            } catch (Exception e1) {
            }
            log.error("Error received: " + hce.getResponseBodyAsString(), hce);
        } catch(JsonProcessingException e) {
            log.error(String.valueOf(e));
            try {
                log.warn("Error Response: " + mapper.writeValueAsString(response));
            } catch (Exception e1) {
            }
        }
        return response;
    }
}

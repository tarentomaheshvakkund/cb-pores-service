package com.igot.cb.demand.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Configuration
@AllArgsConstructor
@NoArgsConstructor
public class StatusTransitionConfig {
    private Map<String, Map<String, Set<String>>> transitions = new HashMap<>();

    public StatusTransitionConfig(String configFilePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Resource resource = new ClassPathResource(configFilePath);
        try (InputStream inputStream = resource.getInputStream()) {
            JsonNode configNode = objectMapper.readTree(inputStream);

            configNode.fields().forEachRemaining(entry -> {
                String requestType = entry.getKey();
                JsonNode statusMapNode = entry.getValue();
                Map<String, Set<String>> statusMap = new HashMap<>();

                statusMapNode.fields().forEachRemaining(statusEntry -> {
                    String currentStatus = statusEntry.getKey();
                    Set<String> validTransitions = new HashSet<>();
                    statusEntry.getValue().forEach(node -> validTransitions.add(node.asText()));
                    statusMap.put(currentStatus, validTransitions);
                });

                transitions.put(requestType, statusMap);
            });
        }
    }

    public boolean isValidTransition(String requestType, String currentStatus, String newStatus) {
        return transitions.containsKey(requestType) &&
                transitions.get(requestType).containsKey(currentStatus) &&
                transitions.get(requestType).get(currentStatus).contains(newStatus);
    }
}

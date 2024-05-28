package com.igot.cb.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.demand.service.DemandServiceImpl;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.transactional.model.Config;
import com.igot.cb.transactional.model.NotificationAsyncRequest;
import com.igot.cb.transactional.model.Template;
import com.igot.cb.transactional.service.RequestHandlerServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.StringWriter;
import java.util.*;

@Component
public class NotificationConsumer {
    private ObjectMapper mapper = new ObjectMapper();

    private Logger logger = LoggerFactory.getLogger(DemandServiceImpl.class);

    @Autowired
    private RequestHandlerServiceImpl requestHandlerService;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private CbServerProperties configuration;

    @KafkaListener(groupId = "${kafka.topic.demand.content.group}",topics = "${kafka.topic.demand.request}")
    public void demandContentConsumer(ConsumerRecord<String, String> data) {
        try {
            Map<String, Object> demandRequest = mapper.readValue(data.value(), HashMap.class);
            processNotification(demandRequest);
        } catch(Exception e) {
            logger.error("Failed to process demand request. Message received : " + data.value(), e);
        }
    }

    public void processNotification(Map<String,Object> demandRequest){
        try {
            long startTime = System.currentTimeMillis();

            Set<String> providerRootOrgIds = new HashSet<>((List<String>) demandRequest.get(Constants.PROVIDER_EMAIL_ID_LIST));
            String mdoAdminId = (String) demandRequest.get(Constants.CREATED_BY);
                Map<String, Object> mailNotificationDetails = new HashMap<>();
                mailNotificationDetails.put(Constants.PROVIDER_EMAIL_ID_LIST, demandRequest.get(Constants.PROVIDER_EMAIL_ID_LIST));
                mailNotificationDetails.put(Constants.MDO_NAME, demandRequest.get(Constants.MDO_NAME));
                mailNotificationDetails.put(Constants.COMPETENCY_AREA, demandRequest.get(Constants.COMPETENCY_AREA));
                mailNotificationDetails.put(Constants.COMPETENCY_THEMES, demandRequest.get(Constants.COMPETENCY_THEMES));
                mailNotificationDetails.put(Constants.COMPETENCY_SUB_THEMES, demandRequest.get(Constants.COMPETENCY_SUB_THEMES));
                mailNotificationDetails.put(Constants.DESCRIPTION , demandRequest.get(Constants.DESCRIPTION));
                mailNotificationDetails.put(Constants.CREATED_BY, demandRequest.get(Constants.CREATED_BY));
                mailNotificationDetails.put(Constants.DEMAND_ID, demandRequest.get(Constants.DEMAND_ID));
                sendNotificationToProvidersAsync(mailNotificationDetails);
            logger.info(String.format("Completed request for content. Time taken: ", (System.currentTimeMillis() - startTime)));
        } catch (Exception e) {
            logger.error("Exception occurred while sending email : " + e.getMessage(), e);
        }
    }

    private String constructEmailTemplate(String templateName, Map<String, Object> params) {
        String replacedHTML = new String();
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.NAME, templateName);
            List<Map<String, Object>> templateMap = cassandraOperation.getRecordsByPropertiesByKey(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_EMAIL_TEMPLATE, propertyMap, Collections.singletonList(Constants.TEMPLATE),null);
            String htmlTemplate = templateMap.stream()
                    .findFirst()
                    .map(template -> (String) template.get(Constants.TEMPLATE))
                    .orElse(null);
            VelocityEngine velocityEngine = new VelocityEngine();
            velocityEngine.init();
            VelocityContext context = new VelocityContext();
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                context.put(entry.getKey(), entry.getValue());
            }
            StringWriter writer = new StringWriter();
            velocityEngine.evaluate(context, writer, "HTMLTemplate", htmlTemplate);
            replacedHTML = writer.toString();
        } catch (Exception e) {
            logger.error("Unable to create template ", e);
        }
        return replacedHTML;
    }

    private void sendNotificationToProvidersAsync(Map<String, Object> mailNotificationDetails){
        List<String> providerIdList = (List<String>) mailNotificationDetails.get(Constants.PROVIDER_EMAIL_ID_LIST);
        String mdoName = (String) mailNotificationDetails.get(Constants.MDO_NAME);

        Map<String, Object> params = new HashMap<>();
        NotificationAsyncRequest notificationAsyncRequest = new NotificationAsyncRequest();
        notificationAsyncRequest.setPriority(1);
        notificationAsyncRequest.setType(Constants.EMAIL);
        notificationAsyncRequest.setIds(providerIdList);

        params.put(Constants.MDO_NAME_PARAM, mdoName);
        params.put(Constants.NAME, mdoName);
        params.put(Constants.COMPETENCY_AREA_PARAM, mailNotificationDetails.get(Constants.COMPETENCY_AREA));
        params.put(Constants.COMPETENCY_THEME_PARAM, mailNotificationDetails.get(Constants.COMPETENCY_THEMES));
        params.put(Constants.COMPETENCY_SUB_THEME_PARAM, mailNotificationDetails.get(Constants.COMPETENCY_SUB_THEMES));
        params.put(Constants.DESCRIPTION, mailNotificationDetails.get(Constants.DESCRIPTION));
        params.put(Constants.FROM_EMAIL, configuration.getSupportEmail());
        params.put(Constants.ORG_NAME, mdoName);
        Template template = new Template(constructEmailTemplate(configuration.getDemandRequestTemplate(), params),configuration.getDemandRequestTemplate(), params);

        Config config = new Config();
        config.setSubject(Constants.REQUEST_CONTENT_SUBJECT);
        config.setSender(configuration.getSupportEmail());

        Map<String, Object> templateMap = new HashMap<>();
        templateMap.put(Constants.CONFIG, config);
        templateMap.put(Constants.TYPE, Constants.EMAIL);
        templateMap.put(Constants.DATA, template.getData());
        templateMap.put(Constants.ID, configuration.getDemandRequestTemplate());
        templateMap.put(Constants.PARAMS, params);

        Map<String, Object> action = new HashMap<>();
        action.put(Constants.TEMPLATE, templateMap);
        action.put(Constants.TYPE, Constants.EMAIL);
        action.put(Constants.CATEGORY, Constants.EMAIL);

        Map<String, Object> createdBy = new HashMap<>();
        createdBy.put(Constants.ID, mailNotificationDetails.get(Constants.CREATED_BY));
        createdBy.put(Constants.TYPE, Constants.MDO);
        action.put(Constants.CREATED_BY, createdBy);
        notificationAsyncRequest.setAction(action);

        Map<String, Object> req = new HashMap<>();
        Map<String, List<NotificationAsyncRequest>> notificationMap = new HashMap<>();
        notificationMap.put(Constants.NOTIFICATIONS, Collections.singletonList(notificationAsyncRequest));
        req.put(Constants.REQUEST, notificationMap);
        sendNotification(req, configuration.getNotificationAsyncPath());
    }

    private void sendNotification(Map<String, Object> request, String urlPath) {
        StringBuilder builder = new StringBuilder();
        builder.append(configuration.getNotifyServiceHost()).append(urlPath);
        try {
            logger.info(mapper.writeValueAsString(request));
            Map<String, Object> response = requestHandlerService.fetchResultUsingPost(builder.toString(), request, null);
            logger.debug("The email notification is successfully sent, response is: " + response);
        } catch (Exception e) {
            logger.error("Exception while posting the data in notification service: ", e);
        }
    }
}

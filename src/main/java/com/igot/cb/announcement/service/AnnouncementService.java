package com.igot.cb.announcement.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.igot.cb.pores.dto.CustomResponse;

public interface AnnouncementService {

  CustomResponse createAnnouncement(JsonNode announcementEntity);
}

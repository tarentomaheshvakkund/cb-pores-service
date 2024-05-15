package com.igot.cb.playlist.util;

import com.igot.cb.pores.util.ApiRespParam;
import com.igot.cb.pores.util.ApiResponse;
import com.igot.cb.pores.util.Constants;
import java.util.UUID;
import org.joda.time.DateTime;
import org.springframework.http.HttpStatus;

public class ProjectUtil {

  public static ApiResponse createDefaultResponse(String api) {
    ApiResponse response = new ApiResponse();
    response.setId(api);
    response.setVer(Constants.API_VERSION_1);
    response.setParams(new ApiRespParam(UUID.randomUUID().toString()));
    response.getParams().setStatus(Constants.SUCCESS);
    response.setResponseCode(HttpStatus.OK);
    response.setTs(DateTime.now().toString());
    return response;
  }

}

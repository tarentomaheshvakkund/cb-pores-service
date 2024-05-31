package com.igot.cb.pores.util;

/**
 * @author Mahesh RV
 */
public class Constants {

  public static final String KEYSPACE_SUNBIRD = "sunbird";
  public static final String KEYSPACE_SUNBIRD_COURSES = "sunbird_courses";
  public static final String CORE_CONNECTIONS_PER_HOST_FOR_LOCAL = "coreConnectionsPerHostForLocal";
  public static final String CORE_CONNECTIONS_PER_HOST_FOR_REMOTE = "coreConnectionsPerHostForRemote";
  public static final String MAX_CONNECTIONS_PER_HOST_FOR_LOCAL = "maxConnectionsPerHostForLocal";
  public static final String MAX_CONNECTIONS_PER_HOST_FOR_REMOTE = "maxConnectionsPerHostForRemote";
  public static final String MAX_REQUEST_PER_CONNECTION = "maxRequestsPerConnection";
  public static final String HEARTBEAT_INTERVAL = "heartbeatIntervalSeconds";
  public static final String POOL_TIMEOUT = "poolTimeoutMillis";
  public static final String CASSANDRA_CONFIG_HOST = "cassandra.config.host";
  public static final String SUNBIRD_CASSANDRA_CONSISTENCY_LEVEL = "LOCAL_QUORUM";
  public static final String EXCEPTION_MSG_FETCH = "Exception occurred while fetching record from ";
  public static final String INSERT_INTO = "INSERT INTO ";
  public static final String DOT = ".";
  public static final String OPEN_BRACE = "(";
  public static final String VALUES_WITH_BRACE = ") VALUES (";
  public static final String QUE_MARK = "?";
  public static final String COMMA = ",";
  public static final String CLOSING_BRACE = ");";
  public static final String INTEREST_ID = "interest_id";
  public static final String RESPONSE = "response";
  public static final String SUCCESS = "success";
  public static final String FAILED = "Failed";
  public static final String ERROR_MESSAGE = "errmsg";
  public static final String DEMAND_ID = "demand_id";
  public static final String DEMAND_ID_RQST = "demandId";
  public static final String USER_ID = "user_id";
  public static final String USER_ID_RQST = "userId";
  public static final String INTEREST_FLAG = "interest_flag";
  public static final String INTEREST_FLAG_RQST = "interestFlag";
  public static final String CREATED_ON = "createdOn";
  public static final String UPDATED_ON = "updatedOn";
  public static final String DATA = "data";
  public static final String DATABASE = "sunbird";
  public static final String TABLE = "interest_capture";
  public static final String REGEX = "^\"|\"$";
  public static final String IS_ACTIVE = "isActive";
  public static final Boolean ACTIVE_STATUS = true;
  public static final String LAST_UPDATED_DATE = "lastUpdatedDate";
  public static final String CREATED_DATE = "createdDate";
  public static final String PAYLOAD_VALIDATION_FILE = "/payloadValidation/demandValidationData.json";
  public static final String INDEX_NAME = "demand_entity";
  public static final String INDEX_TYPE = "_doc";
  public static final String RESULT = "result";
  public static final String FAILED_CONST = "FAILED";
  public static final String ERROR = "ERROR";
  public static final String REDIS_KEY_PREFIX = "cbpores_";
  public static final String KEYWORD = ".keyword";
  public static final String ASC = "asc";
  public static final String REQUEST_PAYLOAD = "requestPayload";
  public static final String JWT_SECRET_KEY = "demand_search_result";
  public static final String PAYLOAD_VALIDATION_FILE_CONTENT_PROVIDER = "/payloadValidation/contentProviderValidation.json";
  public static final String CONTENT_PROVIDER_ID = "id";
  public static final String INTEREST_COUNT = "interestCount";
  public static final String INTERESTS = "demand_search_result";
  public static final String DOT_SEPARATOR = ".";
  public static final String SHA_256_WITH_RSA = "SHA256withRSA";
  public static final String UNAUTHORIZED = "Unauthorized";
  public static final String SUB = "sub";
  public static final String SSO_URL = "sso.url";
  public static final String SSO_REALM = "sso.realm";
  public static final String ACCESS_TOKEN_PUBLICKEY_BASEPATH = "accesstoken.publickey.basepath";
  public static final String NO_DATA_FOUND = "No data found";
  public static final String SUCCESSFULLY_CREATED = "successfully created";
  public static final String ID = "id";
  public static final String SUCCESSFULLY_READING = "successfully read";
  public static final String ID_NOT_FOUND = "Id not found";
  public static final String INVALID_ID = "Invalid Id";
  public static final String DELETED_SUCCESSFULLY = "deleted successfully";
  public static final String ALREADY_INACTIVE = "already inactive Id";
  public static final String ERROR_WHILE_DELETING_DEMAND = "Error while deleting demand with ID";
  public static final String SUCCESSFULLY_UPDATED = "successfully updated";
  public static final String CONTENT_PARTNER_NOT_FOUND = "content partner not found";
  public static final String FETCH_RESULT_CONSTANT = ".fetchResult:";
  public static final String URI_CONSTANT = "URI: ";
  public static final String OK = "OK";
  public static final String RESPONSE_CODE = "responseCode";
  public static final String CONTENT = "content";
  public static final String LIVE = "Live";
  public static final String STATUS = "status";
  public static final String NAME = "name";
  public static final String COMPETENCIES_V5 = "competencies_v5";
  public static final String AVG_RATING = "avgRating";
  public static final String ORG_ID = "orgId";
  public static final String CHILDREN = "children";
  public static final String API_VERSION_1 = "1.0";
  public static final String API_PLAYLIST_CREATE = "api.playlist.create";
  public static final String API_PLAYLIST_READ = "api.playlist.read";
  public static final Object CREATED = "Created";
  public static final String IDENTIFIER = "identifier";
  public static final String DESCRIPTION = "description";
  public static final String ADDITIONAL_TAGS = "additionalTags";
  public static final String CONTENT_TYPE_KEY = "contentType";
  public static final String PRIMARY_CATEGORY = "primaryCategory";
  public static final String DURATION = "duration";
  public static final String COURSE_APP_ICON = "appIcon";
  public static final String POSTER_IMAGE = "posterImage";
  public static final String ORGANISATION = "organisation";
  public static final String CREATOR_LOGO = "creatorLogo";
  public static final String NOT_FOUND = "Not found";
  public static final String FILTERS = "filters";
  public static final String REQUEST_TYPE = "requestType";
  public static final String RQST_CONTENT_TYPE = "type";
  public static final String PLAY_LIST_VALIDATION_FILE_JSON = "/payloadValidation/playListValidationFile.json";
  public static final String ORG_FEATURED_COURSE_KEY = "ORG_FEATURED_COURSES";
  public static final String ORG_COURSE_NOT_FOUND = "Not found course for this org";
  public static final String API_PLAYLIST_UPDATED = "api.playlist.updated";
  public static final Object UPDATED = "Updated";
  public static final String INTEREST_VALIDATION_FILE_JSON = "/payloadValidation/interestPayloadValidation.json";
  public static final String REQUESTED = "Requested";
  public static final String INTEREST_INDEX_NAME = "interests";
  public static final String INTEREST_ID_RQST = "interestId";
  public static final String ASSIGNED_PROVIDER = "assignedProvider";
  public static final String ASSIGNED_BY = "assignedBy";
  public static final String GRANTED = "Granted";
  public static final String ASSIGNED = "Assigned";
  public static final String SUCCESSFULLY_ASSIGNED = "Successfully Assigned interest with demand";
  public static final String OWNERID = "ownerId";
  public static final String X_AUTH_USER_ORG_ID = "x-authenticated-user-orgid";
  public static final String X_AUTH_TOKEN = "x-authenticated-user-token";
  public static final String USER_ID_DOESNT_EXIST = "User Id doesn't exist! Please supply a valid auth token";
  public static final String TABLE_USER = "user";
  public static final String ROOT_ORG_ID= "rootOrgId";
  public static final String USER_ROOT_ORG_ID ="rootorgid";
  public static final String ROOT_ORG_ID_DOESNT_MATCH = "Unauthorized User.";
  public static final String OWNER = "owner";
  public static final String STATUS_TRANSITION_PATH= "/payloadValidation/statusTransitions.json";
  public static final String BROADCAST ="Broadcast";
  public static final String UNASSIGNED ="Unassigned";
  public static final String INVALID_STATUS_TRANSITION = "Requesting with invalid status";
  public static final String NEW_STATUS ="newStatus";
  public static final String MISSING_ID_OR_NEW_STATUS="demand id and newStatus are required for updating demand";
  public static final String CANNOT_UPDATE_INACTIVE_DEMAND="Cannot update inactive demand";
  public static final String USER_ROOT_ORG_NAME="orgname";
  public static final String ORG_TABLE = "organisation";
  public static final String ORG_NAME = "orgName";
  public static final String PROVIDER_ID = "providerId";
  public static final String PROVIDER_NAME = "providerName";
  public static final String PREV_ASSIGNED_PROVIDER = "previousAssignedProvider";
  public static final String ANNOUNCEMENT_VALIDATION_FILE_JSON = "/payloadValidation/announcementValidation.json";
  public static final String ANNOUNCEMENT_ID = "announcementId";
  public static final String ANNOUNCEMENT_INDEX = "announcement_entity";
  public static final String PLAYLIST_INDEX_NAME = "playlist";
  public static final String KEY_PLAYLIST = "playListKey";
  public static final String ACTIVE = "Active";
  public static final String IN_ACTIVE = "Inactive";
  private Constants() {
  }
}

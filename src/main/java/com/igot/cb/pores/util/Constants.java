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
    public static final String  LAST_UPDATED_DATE= "lastUpdatedDate";
    public static final String CREATED_DATE = "createdDate";
    public static final String PAYLOAD_VALIDATION_FILE = "/payloadValidation/demandValidationData.json";
    public static final String INDEX_NAME ="demand_entity";
    public static final String INDEX_TYPE = "_doc";
    public static final String RESULT = "result";
    public static final String FAILED_CONST = "FAILED";
    public static final String ERROR = "ERROR";
    public static final String REDIS_KEY_PREFIX = "demand_";
    public static final String KEYWORD = ".keyword";
    public static final String ASC = "asc";
    public static final String REQUEST_PAYLOAD ="requestPayload";
    public static final String JWT_SECRET_KEY ="demand_search_result";
    public static final String PAYLOAD_VALIDATION_FILE_CONTENT_PROVIDER ="/payloadValidation/contentProviderValidation.json";
    public static final String CONTENT_PROVIDER_ID ="id";
    public static final String INTEREST_COUNT ="demand_search_result";
    public static final String INTERESTS ="demand_search_result";
    public static final String No_DATA_FOUND ="No data found";
    public static final String SUCCESSFULLY_CREATED ="Successfully created";
    private Constants() {
    }
}

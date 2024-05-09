package com.igot.cb.pores.exceptions;

/**
 * @author Mahesh RV
 * @author Ruksana
 * <p>
 * Custom exception class for handling common project exceptions.
 * Extends RuntimeException to indicate unchecked exceptions.
 * Provides methods to access exception details such as code, message, and response code.
 */
public class ProjectCommonException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String code;
    private final String message;
    private final int responseCode;

    /**
     * Constructs a new ProjectCommonException with the specified code, message, and response code.
     *
     * @param code         The error code associated with the exception.
     * @param message      The error message associated with the exception.
     * @param responseCode The HTTP response code to be returned.
     */
    public ProjectCommonException(String code, String message, int responseCode) {
        super();
        this.code = code;
        this.message = message;
        this.responseCode = responseCode;
    }

    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public int getResponseCode() {
        return responseCode;
    }

}
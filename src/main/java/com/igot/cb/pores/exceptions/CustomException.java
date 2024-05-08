package com.igot.cb.pores.exceptions;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CustomException extends RuntimeException {
  private String code;
  private String message;
  private int httpStatusCode;

  public CustomException(String code, String message) {
    this.code = code;
    this.message = message;
  }

  public CustomException(String code, String message, int httpStatusCode) {
    this.code = code;
    this.message = message;
    this.httpStatusCode = httpStatusCode;
  }

  public CustomException() {

  }
}

package com.yunli.bigdata.example.exception;

/**
 * @Description: 自定义全局异常处理类
 */
public class SdkException extends RuntimeException {

  private Integer code;

  private String message;

  public SdkException(Integer code, String message) {
    super(message);
    this.code = code;
    this.message = message;
  }

  public SdkException(ErrorCode errorCode) {
    super(errorCode.getMessage());
    this.code = errorCode.getCode();
    this.message = errorCode.getMessage();
  }

  public SdkException(ErrorCode errorCode, Object... args) {
    super(String.format(errorCode.getMessage(), args));
    this.code = errorCode.getCode();
    this.message = super.getMessage();
  }

  public SdkException(Integer code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
    this.message = message;
  }

  public Integer getCode() {
    return code;
  }

  public void setCode(Integer code) {
    this.code = code;
  }

  @Override
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}

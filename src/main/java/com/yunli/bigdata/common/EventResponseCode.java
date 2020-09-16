package com.yunli.bigdata.common;

public final class EventResponseCode {

  public static final int SUCCESS_CODE = 200;

  public static final String SUCCESS_MSG = "OK";

  // 请求的参数不正确，或缺少必须的参数（token除外）
  public static final int BAD_REQUEST_CODE = 400;

  public static final String BAD_REQUEST_MSG = "Bad Request";

  // token不通过
  public static final int UNAUTHORIZED_CODE = 401;

  public static final String UNAUTHORIZED_MSG = "Unauthorized";

  // token有效，进一步校验失败
  public static final int FORBIDDEN_CODE = 403;

  public static final String FORBIDDEN_MSG = "Forbidden";

  public static final int NOT_FOUND_CODE = 404;

  public static final String NOT_FOUND_MSG = "Not Found";

  public static final int EXISTS_CODE = 422;

  public static final String EXISTS_MSG = "Exists";

  // 内部错误（返回定位问题的错误，不抛出trace）
  public static final int INTERNAL_SERVER_ERROR_CODE = 500;

  public static final String INTERNAL_SERVER_ERROR_MSG = "Internal Server Error";

  // 服务未就绪之前
  public static final int SERVICE_UNAVAILABLE_CODE = 503;

  public static final String SERVICE_UNAVAILABLE_MSG = "Service Unavailable";
}

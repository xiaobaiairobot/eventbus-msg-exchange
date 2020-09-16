package com.yunli.bigdata.util;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * @author : jiangxunyu
 * @Description: 定义结果返回类
 * @CreateDate: 2019/6/12 16:46
 */
@ApiModel(description = "响应结果")
public class Result<T> {

  @ApiModelProperty("响应码")
  private int code;

  @ApiModelProperty("响应消息")
  private String message;

  @ApiModelProperty("响应数据")
  private T data;

  public static final int SUCCESS_CODE = 200;

  public static final String SUCCESS_MSG = "success";

  public static final int CREATE_CODE = 201;

  public Result() {
  }

  public Result(int code, String message, T data) {
    this.code = code;
    this.message = message;
    this.data = data;
  }

  @JsonIgnore
  public boolean isSuccess() {
    return code == SUCCESS_CODE || code == CREATE_CODE;
  }

  public static <T> Result<T> success(T data) {
    return new Result<>(SUCCESS_CODE, SUCCESS_MSG, data);
  }

  public static <T> Result<T> success(int code, T data) {
    return new Result<>(code, SUCCESS_MSG, data);
  }

  public static <T> Result<T> error(int code, String message) {
    return new Result<>(code, message, null);
  }

  public int getCode() {
    return code;
  }

  public void setCode(int code) {
    this.code = code;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }
}

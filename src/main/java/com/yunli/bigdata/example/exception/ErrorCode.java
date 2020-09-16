package com.yunli.bigdata.example.exception;

/**
 * 全局MessageCode接口
 *
 * @author david
 */
public interface ErrorCode {

  // 错误码编号
  int getCode();

  // 错误码描述
  String getMessage();
}

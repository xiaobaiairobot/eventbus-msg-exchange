package com.yunli.bigdata.common;


import com.yunli.bigdata.example.exception.ErrorCode;

/**
 * 全局Message
 *
 * @author david
 */
public enum CommonMessageCode implements ErrorCode {
  /**
   *
   */
  INFO_200(200, "成功"),
  INFO_201(201, "已创建"),
  ERROR_401(401, "无权操作"),
  ERROR_500(500, "系统异常"),

  // 参数校验Code定义：1000 ~ 1099
  ERROR_1001(1001, "%s不能为空"),
  ERROR_1002(1002, "%s的值只能为[%s]"),
  ERROR_1003(1003, "%s的长度不能超过[%s]"),
  ERROR_1004(1004, "%s[%s]无效"),
  ERROR_1005(1005, "%s不能大于%s[%s，%s]"),
  ERROR_1006(1006, "%s不能重复"),
  ERROR_1007(1007, "%s必须是正整数"),
  ERROR_1008(1008, "%s[%s]格式不正确"),
  ERROR_1009(1009, "%s[%s]不能包含[%s]等字符"),

  // 共通业务Code定义：1100 ~ 1199
  ERROR_1100(1100, "%s[%s]已经存在"),
  ERROR_1101(1101, "%s[%s]不存在"),
  ERROR_1102(1102, "%s[%s]不能重复"),
  ERROR_1103(1103, "结束时间不能小于开始时间"),
  ERROR_1104(1104, "登陆已过期,请重新登录"),
  ERROR_1105(1105, "%s[%s]不支持"),
  ERROR_1106(1106, "底座[%s]访问异常"),
  ;

  private int code;

  private String message;

  CommonMessageCode(int code, String message) {
    this.code = code;
    this.message = message;
  }

  @Override
  public int getCode() {
    return code;
  }

  public void setCode(int code) {
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

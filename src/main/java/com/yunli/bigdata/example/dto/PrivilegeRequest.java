package com.yunli.bigdata.example.dto;

/**
 * @author david
 * @date 2020/8/27 11:06 上午
 */
public class PrivilegeRequest {
  private String resource;
  private String action;

  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }
}

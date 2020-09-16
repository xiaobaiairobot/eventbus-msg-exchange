package com.yunli.bigdata.example.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author david
 * @date 2020/8/28 10:30 上午
 */
@Configuration
public class CredentialConfiguration {

  @Value("${spring.credential.key}")
  private String key;

  @Value("${spring.credential.signature}")
  private String signature;

  @Value("${spring.credential.expiredDate}")
  private String expiredDate;

  @Value("${spring.credential.appId}")
  private String appId;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getSignature() {
    return signature;
  }

  public void setSignature(String signature) {
    this.signature = signature;
  }

  public String getExpiredDate() {
    return expiredDate;
  }

  public void setExpiredDate(String expiredDate) {
    this.expiredDate = expiredDate;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }
}

package com.yunli.bigdata.example.dto;


import java.util.List;
import java.util.Set;

import com.yunli.bigdata.eventbus.sdk.AccessCredential.Privilege;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * @author david
 * @date 2020/8/7 2:17 下午
 */
@ApiModel(description = "消息发送请求")
public class CreateCredentialMsgRequest {

  @ApiModelProperty(value = "服务器地址")
  private String server;

  @ApiModelProperty(value = "发送端口")
  private Long producerPort;

  @ApiModelProperty(value = "信道编码")
  private String sourceTopic;

  @ApiModelProperty(value = "发送次数")
  private Integer sendTimes;

  @ApiModelProperty(value = "列类型，格式范例：long,string,long,string,string")
  private String columns;

  @ApiModelProperty(value = "证书KEY")
  private String key;

  @ApiModelProperty(value = "证书签名")
  private String signature;

  @ApiModelProperty(value = "过期时间")
  private String expiredDate;

  @ApiModelProperty(value = "APP编码")
  private String appId;

  @ApiModelProperty(value = "权限信息")
  private List<PrivilegeRequest> privileges;


  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public Long getProducerPort() {
    return producerPort;
  }

  public void setProducerPort(Long producerPort) {
    this.producerPort = producerPort;
  }

  public String getSourceTopic() {
    return sourceTopic;
  }

  public void setSourceTopic(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }

  public Integer getSendTimes() {
    return sendTimes;
  }

  public void setSendTimes(Integer sendTimes) {
    this.sendTimes = sendTimes;
  }

  public String getColumns() {
    return columns;
  }

  public void setColumns(String columns) {
    this.columns = columns;
  }

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

  public List<PrivilegeRequest> getPrivileges() {
    return privileges;
  }

  public void setPrivileges(List<PrivilegeRequest> privileges) {
    this.privileges = privileges;
  }
}

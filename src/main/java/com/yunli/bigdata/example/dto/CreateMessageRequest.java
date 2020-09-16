package com.yunli.bigdata.example.dto;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * @author david
 * @date 2020/8/7 2:17 下午
 */
@ApiModel(description = "消息发送请求")
public class CreateMessageRequest {

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
}

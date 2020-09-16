package com.yunli.bigdata.example.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @author david
 * @date 2020/7/28 7:43 下午
 */
@Configuration
public class EventBusConfiguration {

  @Value("${spring.event-bus.server-ip}")
  private String server;

  @Value("${spring.event-bus.producer-port}")
  private Long producerPort;

  @Value("${spring.event-bus.consumer-port}")
  private Long consumerPort;

  @Value("${spring.event-bus.source-topic}")
  private String sourceTopic;

  @Value("${spring.event-bus.target-topic}")
  private String targetTopic;

  @Value("${spring.event-bus.consumer-group}")
  private String consumerGroup;

  @Value("${spring.event-bus.send-times}")
  private Integer sendTimes;

  @Value("${spring.event-bus.columns}")
  private String columns;

  public String getSourceTopic() {
    return sourceTopic;
  }

  public void setSourceTopic(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }

  public String getTargetTopic() {
    return targetTopic;
  }

  public void setTargetTopic(String targetTopic) {
    this.targetTopic = targetTopic;
  }

  public Integer getSendTimes() {
    return sendTimes;
  }

  public void setSendTimes(Integer sendTimes) {
    this.sendTimes = sendTimes;
  }

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

  public Long getConsumerPort() {
    return consumerPort;
  }

  public void setConsumerPort(Long consumerPort) {
    this.consumerPort = consumerPort;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public void setConsumerGroup(String consumerGroup) {
    this.consumerGroup = consumerGroup;
  }

  public String getColumns() {
    return columns;
  }

  public void setColumns(String columns) {
    this.columns = columns;
  }
}

package com.yunli.bigdata.example.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * @author david
 * @date 2020/7/29 11:10 上午
 */
@Configuration
public class TargetTopicConfiguration implements InitializingBean {

  private final EventBusConfiguration eventBusConfiguration;

  @Autowired
  public TargetTopicConfiguration(EventBusConfiguration eventBusConfiguration) {
    this.eventBusConfiguration = eventBusConfiguration;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    System.setProperty("targetTopicName", eventBusConfiguration.getTargetTopic());
  }
}

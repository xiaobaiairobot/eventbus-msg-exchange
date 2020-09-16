package com.yunli.bigdata.example.service.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yunli.bigdata.eventbus.sdk.AccessCredential;
import com.yunli.bigdata.eventbus.sdk.Consumer;
import com.yunli.bigdata.eventbus.sdk.ConsumerGroup;
import com.yunli.bigdata.eventbus.sdk.Event;

/**
 * @author david
 * @date 2020/9/16 10:27 上午
 */
public abstract class EventBusConsumer extends Consumer {
  private final Logger logger = LoggerFactory.getLogger(EventBusConsumer.class);

  // 消息过滤器中使用
  private final Pattern EVENT_FILTER_PATTERN = Pattern.compile(".*[\\\"]+deviceId[\\\"]+:[\\\"]+([a-zA-Z0-9-]+)[\\\"]+.*");

  // 设备白名单
  private List<String> deviceWhiteList;

  public EventBusConsumer(String host, int port, ConsumerGroup consumerGroup, List<Integer> partitions,
      AccessCredential accessCredential, List<String> deviceWhiteList) {
    super(host, port, consumerGroup, partitions, accessCredential);
    this.deviceWhiteList = deviceWhiteList;
  }

  public EventBusConsumer(List<String> etcdEndpoints, ConsumerGroup consumerGroup,
      List<Integer> partitions, AccessCredential accessCredential) {
    super(etcdEndpoints, consumerGroup, partitions, accessCredential);
  }

  @Override
  protected void onError(Throwable t) {
    t.printStackTrace();
    try {
      this.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void onEvent(Event e) {
    if (eventFilter(e)) {
      onMessage(e);
    }
  }

  /**
   * 业务模块实现此方法获取消息
   * @param e
   */
  public abstract void onMessage(Event e);


  /**
   * 该过滤器是一个范例，实现不完整，你需要完善该过滤器以实现具体业务
   * @param e
   * @return
   */
  public boolean eventFilter(Event e) {
    String message = new String(e.getData(), StandardCharsets.UTF_8);
    logger.info("receive message in filter {}", message);
    Matcher matcher = EVENT_FILTER_PATTERN.matcher(message);
    while (matcher.find()) {
      String key = matcher.group(1);
      if (deviceWhiteList != null && deviceWhiteList.contains(key)) {
        return true;
      }
    }
    return false;
  }

  public static void main(String[] args) {
    String message = "\"{\"\"deviceId\"\":\"\"abc-device-04\"\",\"\"deviceInfo\"\":\"\"device_info_message_desc\"\"}\"";
    Pattern EVENT_FILTER_PATTERN = Pattern.compile(".*[\\\"]+deviceId[\\\"]+:[\\\"]+([a-zA-Z0-9-]+)[\\\"]+.*");
    Matcher matcher = EVENT_FILTER_PATTERN.matcher(message);

    while (matcher.find()) {
      System.out.println(matcher.groupCount());
      System.out.println(matcher.group(1));
    }
  }
}
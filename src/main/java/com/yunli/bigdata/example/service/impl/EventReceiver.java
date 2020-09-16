package com.yunli.bigdata.example.service.impl;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.google.gson.Gson;
import com.yunli.bigdata.common.CommonMessageCode;
import com.yunli.bigdata.eventbus.sdk.AccessCredential;
import com.yunli.bigdata.eventbus.sdk.AccessCredential.AccessCredentialData;
import com.yunli.bigdata.eventbus.sdk.AccessCredential.Privilege;
import com.yunli.bigdata.eventbus.sdk.Consumer;
import com.yunli.bigdata.eventbus.sdk.ConsumerGroup;
import com.yunli.bigdata.eventbus.sdk.Event;
import com.yunli.bigdata.example.config.CredentialConfiguration;
import com.yunli.bigdata.example.config.EventBusConfiguration;
import com.yunli.bigdata.example.exception.SdkException;
import com.yunli.bigdata.util.DateUtil;

/**
 * @author david
 * @date 2020/7/28 7:54 下午
 */
@Component
public class EventReceiver {

  private final Logger logger = LoggerFactory.getLogger(EventReceiver.class);

  private final EventBusConfiguration eventBusConfiguration;

  private final CredentialConfiguration credentialConfiguration;

  private final Consumer consumer;

  private final EventSender eventSender;


  @Autowired
  public EventReceiver(EventBusConfiguration eventBusConfiguration,
      CredentialConfiguration credentialConfiguration,
      EventSender eventSender) {
    this.eventBusConfiguration = eventBusConfiguration;
    this.credentialConfiguration = credentialConfiguration;
    this.eventSender = eventSender;
    int port = this.eventBusConfiguration.getConsumerPort().intValue();
    List<Integer> parts = new ArrayList<>();
    parts.add(0);
    parts.add(1);

    AccessCredential accessCredential = null;
    if (!StringUtils.isEmpty(credentialConfiguration.getSignature())) {
      Date dtExpired = null;
      try {
        dtExpired = DateUtil.fromFullString(credentialConfiguration.getExpiredDate());
      } catch (ParseException e) {
        e.printStackTrace();
        throw new SdkException(CommonMessageCode.ERROR_1004, "expiredDate", credentialConfiguration.getExpiredDate());
      }
      Set<Privilege> setPrivilege = new HashSet<Privilege>();
      setPrivilege.add(new Privilege(eventBusConfiguration.getSourceTopic(), "consume"));
      accessCredential = new AccessCredential(new AccessCredentialData(
          credentialConfiguration.getKey(),
          dtExpired,
          credentialConfiguration.getAppId(),
          setPrivilege,
          credentialConfiguration.getSignature()
      ));
    }
    List<String> allowDevices = new ArrayList<>();
    allowDevices.add("abc-device-01");
    allowDevices.add("abc-device-02");
    this.consumer = new EventBusConsumer(eventBusConfiguration.getServer(), port,
        new ConsumerGroup(this.eventBusConfiguration.getConsumerGroup(),
            this.eventBusConfiguration.getSourceTopic()), parts, accessCredential, allowDevices) {

      @Override
      public void onMessage(Event e) {
        String message = new String(e.getData(), StandardCharsets.UTF_8);
        logger.info("from topic: {},partition: {}, message: {}", e.getTopic(), e.getPartitionKey(), message);
        // 实现csv转换json的操作
        CSVFormat format = CSVFormat.DEFAULT;
        try {
          CSVParser csvParser = new CSVParser(new StringReader(message), format);
          for (CSVRecord next : csvParser) {
            String json = transportOneRecord(next);
            // 然后将json数据发送出去
            sendJsonToTargetTopic(json);
          }
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    };
  }

  /**
   * 将json数据发送到目标通道
   * @param json
   */
  private void sendJsonToTargetTopic(String json) {
    try {
      eventSender.sendEvent(eventBusConfiguration, json);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public String transportOneRecord(CSVRecord record) {
    // 获取csv中的列名，将csv转换成json应该是；
    // name sex
    // zhangsan nan
    // {"name":"zhangsan","sex":"nan"}
    String columns = this.eventBusConfiguration.getColumns();
    String[] columnList = columns.split(",");
    Map<String, Object> mapRecord = new HashMap<>();
    // 列的配置注意要一致
    for (int i = 0; i < columnList.length; i++) {
      if (i > record.size() - 1) {
        logger.error("警告：列的数量与数据的列数量不一致");
        continue;
      }
      String value = record.get(i);
      if (StringUtils.isEmpty(value)) {
        mapRecord.put(columnList[i], null);
      } else {
        // 类型应该在topic的schema中指定的，见大数据平台上的实时数据信道添加功能，这里临时判断写死
        mapRecord.put(columnList[i], value);
      }
    }
    return new Gson().toJson(mapRecord);
  }
}

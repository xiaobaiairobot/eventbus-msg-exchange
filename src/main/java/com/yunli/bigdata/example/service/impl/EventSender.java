package com.yunli.bigdata.example.service.impl;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.yunli.bigdata.eventbus.sdk.AccessCredential;
import com.yunli.bigdata.eventbus.sdk.Event;
import com.yunli.bigdata.eventbus.sdk.Producer;
import com.yunli.bigdata.example.config.EventBusConfiguration;

/**
 * @author david
 * @date 2020/8/28 11:32 上午
 */
@Service
public class EventSender {

  private final Logger logger = LoggerFactory.getLogger(EventSender.class);

  private AccessCredential accessCredential;

  private CountDownLatch latch = new CountDownLatch(1);

  /**
   * 用于消息发送行号计数器
   */
  private AtomicInteger index = new AtomicInteger(0);

  public AccessCredential getAccessCredential() {
    return accessCredential;
  }

  public void setAccessCredential(AccessCredential accessCredential) {
    this.accessCredential = accessCredential;
  }

  private Producer producer;


  public void sendEvent(EventBusConfiguration eventBusConfiguration, String message)
      throws IOException, InterruptedException {

    if (producer == null) {
      initProducer(eventBusConfiguration);
      Thread watchThread = new Thread(() -> {
        /**
         * 异常时重建producer
         */
        while (true) {
          try {
            latch.await();
            latch = new CountDownLatch(1);
            producer.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
          initProducer(eventBusConfiguration);
        }
      });
      watchThread.start();
    }
    try {
      producer.sendEventAsync(
          new Event(eventBusConfiguration.getTargetTopic(), message, message.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      logger.warn("消息提交到目标topic:{}异常", eventBusConfiguration.getTargetTopic());
      logger.warn(e.getMessage(), e);
    }
    logger.info("消息提交到目标topic:{}成功", eventBusConfiguration.getTargetTopic());
  }

  /**
   * 发送随机消息，用于测试
   * @param eventBusConfiguration
   * @throws IOException
   * @throws InterruptedException
   */
  public void sendEvent(EventBusConfiguration eventBusConfiguration)
      throws IOException, InterruptedException {

    if (producer == null) {
      initProducer(eventBusConfiguration);

      Thread watchThread = new Thread(() -> {
        /**
         * 异常时重建producer
         */
        while (true) {
          try {
            latch.await();
            latch = new CountDownLatch(1);
            producer.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
          initProducer(eventBusConfiguration);
        }
      });
      watchThread.start();
    }

    for (int i = 0; i < eventBusConfiguration.getSendTimes(); i++) {
      String msg = getRandomDeviceMessage(eventBusConfiguration);
      try {
        producer.sendEventAsync(
            new Event(eventBusConfiguration.getSourceTopic(), msg, msg.getBytes(StandardCharsets.UTF_8)));
      } catch (Exception e) {
        logger.warn("消息提交异常");
        logger.warn(e.getMessage(), e);
      }
    }
    logger.info("消息提交成功");
    index.set(0);
    Thread.sleep(500);
  }

  private void initProducer(EventBusConfiguration eventBusConfiguration) {
    producer = new Producer(eventBusConfiguration.getServer(), eventBusConfiguration.getProducerPort().intValue(),
        getAccessCredential()) {
      @Override
      protected void onError(Throwable throwable) {
        logger.warn(throwable.getMessage(), throwable);
        latch.countDown();
      }
    };
  }

  /**
   * 生成随机消息
   * @param eventBusConfiguration
   * @return
   */
  private String getRandomDeviceMessage(EventBusConfiguration eventBusConfiguration) {
    // 拼装json格式的随机消息
    Map<String, String> mapMessage = new HashMap<>();
    int i = new Random().nextInt(5);
    mapMessage.put("deviceId", String.format("abc-device-0%d", i));
    mapMessage.put("deviceInfo", "device_info_message_desc");
    String strJson = new Gson().toJson(mapMessage);
    System.out.println(strJson);
    // toCSV，南部新城的csv是把整个json弄成一列字符串的模式了
    CSVFormat csvFormat = CSVFormat.DEFAULT.withRecordSeparator(System.lineSeparator())
        .withQuoteMode(QuoteMode.NON_NUMERIC);
    final StringWriter out = new StringWriter();
    // CSV中只有一列
    Object[] output = new Object[1];
    // 赋值
    output[0] = strJson;
    try (CSVPrinter csvPrinter = new CSVPrinter(out, csvFormat);) {
      csvPrinter.printRecord(output);
      return out.toString().trim();
    } catch (IOException e) {
      e.printStackTrace();
      logger.error("Unable to format the Iterable to CSVRecord. ", e);
    } finally {
      try {
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return null;
  }
}

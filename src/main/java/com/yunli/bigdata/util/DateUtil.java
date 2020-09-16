package com.yunli.bigdata.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * @author zhengyangyong
 */
public class DateUtil {
  public static Date fromSimpleString(String dateTime) throws ParseException {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateTime);
  }

  public static Date fromFullString(String dateTime) throws ParseException {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(dateTime);
  }

  public static String toSimpleString(Date date) {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
  }

  public static Date plusDays(Date date, int days) {
    return toDate(toLocalDateTime(date).plusDays(days));
  }

  public static Date plusSeconds(Date date, int seconds) {
    return toDate(toLocalDateTime(date).plusSeconds(seconds));
  }

  public static LocalDateTime toLocalDateTime(Date date) {
    return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
  }

  public static Date toDate(LocalDateTime date) {
    return Date.from(date.atZone(ZoneId.systemDefault()).toInstant());
  }

  public static void main(String[] args) throws ParseException {
    String strInfo = "2020-08-27 12:00:00.000";
    Date dt = DateUtil.fromFullString(strInfo);
    System.out.println(dt);
  }
}

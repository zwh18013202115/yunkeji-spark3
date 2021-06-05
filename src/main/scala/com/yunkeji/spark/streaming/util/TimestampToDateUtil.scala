package com.yunkeji.spark.streaming.util

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

/**
 * @author : zhao
 * @creat : 2021-03-07  22:49
 * @description: 时间转换工具
 *
 */
object TimestampToDateUtil {

  /**
   * @description: 时间戳转换为日期字符串
   * @param timestamp
   * @return
   */
  def timestampToDateString(timestamp: Long): String = {
    var dateStr: String = " "
    //val pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    val pattern = "yyyy-MM-dd HH:mm"
    val dateTime: LocalDateTime = LocalDateTime.ofEpochSecond(timestamp / 1000L, 0, ZoneOffset.ofHours(12))
    dateStr = dateTime.format(DateTimeFormatter.ofPattern(pattern))
    dateStr
  }

  /**
   * @description: 日期字符串转换为时间戳
   * @param strDate
   * @return
   */
  def dateToTimestamp(strDate: String): Long = {
    var time: Long = 0L
    //val pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    val pattern = "yyyy-MM-dd HH"
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val localDateTime = LocalDateTime.parse(strDate, formatter)
    time = localDateTime.toInstant(ZoneOffset.ofHours(12)).toEpochMilli()
    time
  }

  /**
   * 日期字符串转换为时间yyyy-MM-dd
   * @param strDate
   * @return
   */
  def strToDate(strDate:String):LocalDate={
    val pattern = "yyyy-MM-dd"
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val localDate = LocalDate.parse(strDate, formatter)
    localDate
  }
}

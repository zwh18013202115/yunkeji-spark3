package com.yunkeji.spark.util

import org.apache.commons.lang3.time.FastDateFormat

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {
  //会有线程安全问题 测试
  //val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //线程安全
  val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(dateStr: String): Long = {
    //y有线程安全问题
    //将日期转成 时间戳 Long
    val date: Date = sdf.parse(dateStr)
    date.getTime
  }

  def getDate(dateStr: String): Date ={
    val date: Date = sdf.parse(dateStr)
    date
  }
}

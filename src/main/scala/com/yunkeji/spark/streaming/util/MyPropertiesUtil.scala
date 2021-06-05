package com.yunkeji.spark.streaming.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties


/**
 * @author : zhao
 * @creat : 2021-03-07  19:49
 * @description: 读取配置文件的工具类
 *
 */
object MyPropertiesUtil {
  /**
   * @description:
   * @param propertiesName
   * @return prop
   */
  def load(propertiesName: String): Properties = {
    val prop: Properties = new Properties()

    prop.load(
      new InputStreamReader(
        //通过当前的线程获取当前类的加载器
        Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName)
        , StandardCharsets.UTF_8)
    )
    prop
  }

//测试
  def main(args: Array[String]): Unit = {
    val prop: Properties = MyPropertiesUtil.load("config.properties")
    println(prop.getProperty("kafka.broker.list"))
  }
}

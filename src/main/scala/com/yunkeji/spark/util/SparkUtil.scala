package com.yunkeji.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtil {

  def getSparkSession(appName: String = this.getClass.getSimpleName, master: String = "local[6]") = {
    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    spark
  }
}

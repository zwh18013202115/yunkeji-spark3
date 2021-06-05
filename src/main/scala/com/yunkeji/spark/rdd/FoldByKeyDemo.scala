package com.yunkeji.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FoldByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(
      List(
        "spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka",
        "hadoop", "flink", "hive", "flink"
      ), 4)

    val wordOneRDD: RDD[(String, Int)] = rdd.map((_, 1))

    val result: RDD[(String, Int)] = wordOneRDD.foldByKey(0)(_ + _)

    result.saveAsTextFile("dataSpark/fold-out")

  }
}

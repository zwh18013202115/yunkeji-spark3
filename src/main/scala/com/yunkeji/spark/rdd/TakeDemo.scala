package com.yunkeji.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TakeDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 35, 46, 4, 5, 9, 59, 9, 9, 5), 2)

    rdd.take(6)
    /**
     * take(n) 可以多次触发 action 从0号分区开始扫描
     * 先看第一个分区的不够n，再拿第二个分区，依次往下
     */
  }
}

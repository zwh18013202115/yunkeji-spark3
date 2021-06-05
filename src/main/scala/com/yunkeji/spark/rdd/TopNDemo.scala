package com.yunkeji.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TopNDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 35, 46, 4, 5, 9, 59, 9, 9, 5), 2)


    val ints: Array[Int] = rdd.top(6)

    /**
     * 59,46,35,9,9,9
     *
     */
    ints.foreach(println(_))
  }
}

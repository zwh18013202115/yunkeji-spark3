package com.yunkeji.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 35, 46, 4, 5, 9, 59, 9, 9, 5), 2)


    rdd.cache()
    rdd.persist()
    rdd.checkpoint()

    /**
     * driver端  通知 executor
     * true：要等方法处理完 才处理后面的逻辑
     * false：异步的不用等
     */
    rdd.unpersist(true)
    val ints: Array[Int] = rdd.top(6)
  }
}

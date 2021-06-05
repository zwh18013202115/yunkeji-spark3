package com.yunkeji.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 35, 46, 4, 5, 9, 59, 9, 9, 5), 2)
    //val res: Long = rdd.count()

    //自定义 将聚合的结果拉到 driver端
    val longs: Array[Long] = sc.runJob(rdd, (it: Iterator[Int]) => {
      var count: Long = 0L
      while (it.hasNext) {
        count += 1
        it.next()
      }
      count
    })

    val result: Long = longs.sum


    println(result)
    sc.stop()
  }
}

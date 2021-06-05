package com.yunkeji.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(List(1, 5, 1, 51, 5, 2, 1, 3))

    //rdd.distinct()
    //自己实现 局部去重  全局去重
    val rddByKey: RDD[(Int, Null)] = rdd.map((_, null)).reduceByKey((x, _) => x)
    val rddList: RDD[Int] = rddByKey.map(_._1)

    rddList.collect().foreach(println(_))

  }
}

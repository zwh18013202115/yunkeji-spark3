package com.yunkeji.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(List(5, 9, 6, 5, 8, 6), 2)

    /**
     * sordBy会触发action 但是它属于transformation
     * 底层调用 RangePartition  Serialization
     */
    rdd.sortBy(x => x ,true ).foreach(println(_))
  }
}

package com.yunkeji.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CogroupDDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)), 3)
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 3), ("shuke", 2)), 2)

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    val rddValueSum: RDD[(String, Int)] = cogroupRDD.mapValues(t => t._1.sum + t._2.sum)

    rddValueSum.collect().foreach(println(_))
    Thread.sleep(100000000)

    /**
     * (tom,4)
     * (jerry,6)
     * (shuke,2)
     * (kitty,2)
     */
    //cogroupRDD.collect().foreach(ite => println(ite))
    /**
     * (tom,(CompactBuffer(1, 2),CompactBuffer(1)))
     * (jerry,(CompactBuffer(3),CompactBuffer(3)))
     * (shuke,(CompactBuffer(),CompactBuffer(2)))
     * (kitty,(CompactBuffer(2),CompactBuffer()))
     */
    /*  val groupWith: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.groupWith(rdd2)

      val rddValueSum: RDD[(String, Int)] = groupWith.mapValues(t => t._1.sum + t._2.sum)
      rddValueSum.collect().foreach(println(_))
  */
    /**
     * (tom,4)
     * (jerry,6)
     * (shuke,2)
     * (kitty,2)
     */
    //groupWith.collect().foreach(ite => println(ite))

    /**
     * (tom,(CompactBuffer(1, 2),CompactBuffer(1)))
     * (jerry,(CompactBuffer(3),CompactBuffer(3)))
     * (shuke,(CompactBuffer(),CompactBuffer(2)))
     * (kitty,(CompactBuffer(2),CompactBuffer()))
     */

    sc.stop()
  }
}

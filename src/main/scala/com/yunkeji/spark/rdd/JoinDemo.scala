package com.yunkeji.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)), 3)
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 3), ("shuke", 2)), 2)

    /**
     * def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))]{
     * this.cogroup(other, partitioner).flatMapValues( pair =>
     * for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)}   --yield 表示 v ，w 有才返回 否则返回空的iterayor
     * rdd 必须是 kv 类型 才能join
     *
     */

    //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    /**
     * (jerry,(3,3))
     * (tom,(1,1))
     * (tom,(2,1))
     */
    //joinRDD.foreach(println(_))
    //joinRDD.saveAsTextFile("dataSpark/join-out")

    /**
     * 自定义实现 join 功能
     *
     */
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    /**
     * (tom,(CompactBuffer(1, 2),CompactBuffer(1)))
     * (jerry,(CompactBuffer(3),CompactBuffer(3)))
     * (shuke,(CompactBuffer(),CompactBuffer(2)))
     * (kitty,(CompactBuffer(2),CompactBuffer()))
     */
    val flatMapVRDD: RDD[(String, (Int, Int))] = cogroupRDD.flatMapValues(t => {
      for (x <- t._1.iterator; y <- t._2.iterator) yield (x, y)
    })


    /**
     * (jerry,(3,3))
     * (tom,(1,1))
     * (tom,(2,1))
     */
    flatMapVRDD.foreach(println(_))

    sc.stop()
  }
}

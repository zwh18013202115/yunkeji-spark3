package com.yunkeji.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object LeftJoinDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)), 3)
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 3), ("shuke", 2)), 2)


    //    val leftRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    //    leftRDD.foreach(println(_))

    /**
     * (jerry,(3,Some(3)))
     * (tom,(1,Some(1)))
     * (tom,(2,Some(1)))
     * (kitty,(2,None))
     */

    //自己实现
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    val res: RDD[(String, (Int, Any))] = cogroupRDD.flatMapValues(t => {
      if (t._2.isEmpty) {
        for (x <- t._1.iterator) yield (x, None)

      } else {
        for (x <- t._1.iterator; y <- t._2.iterator) yield (x, Some(y))
      }
    })

    /**
     * (jerry,(3,Some(3)))
     * (tom,(1,Some(1)))
     * (tom,(2,Some(1)))
     * (kitty,(2,None))
     */

    res.foreach(println(_))

    sc.stop()
  }
}

package com.yunkeji.spark

import scala.collection.mutable._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import java.util

object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[6]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\teacher.log")

    val subjectAndTeacher: RDD[((String, String), Int)] = rdd.map(line => {
      val arr: Array[String] = line.split("/")
      var url: String = null
      var teacher: String = null
      var subject: String = null
      try {
        url = arr(2)
        teacher = arr(3)
        subject = url.split("[.]")(0)
      } catch {
        case e: Exception =>
      }
      ((subject, teacher), 1)
    }).reduceByKey(_ + _)

    subjectAndTeacher.foreach(println(_))


    //println()

    sc.stop()

  }
}

/**
 * 自定义分区器
 */
class MyPartioner(sub: Array[String]) extends Partitioner {

  //在主构造器中定义分区规则
  private val nameToNum = new HashMap[String, Int]()
  var i = 0
  for (elem <- sub) {
    nameToNum(elem) = i
    i += 1
  }

  override def numPartitions: Int = {
    sub.size
  }

  //在Executor的Task中，suffle之前会调用
  override def getPartition(key: Any): Int = {
    val subject: String = key.asInstanceOf[(String, String)]._1
    nameToNum(subject) //根据学科名称获取对应的分区
  }
}

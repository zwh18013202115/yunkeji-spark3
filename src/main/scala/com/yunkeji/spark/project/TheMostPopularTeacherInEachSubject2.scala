package com.yunkeji.spark.project

import com.yunkeji.spark.partioner.SubjectPartition2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey的方法
 * 容易造成 某个分区数据量 分大 多个学科
 *
 * 优化重分区并在分区内排序
 */
object TheMostPopularTeacherInEachSubject2 {
  def main(args: Array[String]): Unit = {
    val isLocal: Boolean = args(0).toBoolean
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    if (isLocal) {
      conf.setMaster("local[4]")
    }

    val sc = new SparkContext(conf)
    //指定参数从哪里读取数据
    val rdd: RDD[String] = sc.textFile(args(1))
    //val rdd: RDD[String] = sc.textFile("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\teacher.log")

    // val topN: Int = args(2).toInt
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
    }).filter(t => t._1._1 != null || t._1._2 != null).reduceByKey(_ + _)

    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()
    //自定义分区器
    //初始化分区
    val myPartioner = new SubjectPartition2(subjects)

    //定义比较 规则 两种方法
    implicit val rules: Ordering[(String, String, Int)] = Ordering[Int].on[(String, String, Int)](t => -t._3)
    /*   implicit val orderingRules = new Ordering[(String, String, Int)] {
            override def compare(x: (String, String, Int), y: (String, String, Int)): Int = {
              -(x._3-y._3)
            }
          }
   */

    //使用自定义分区器进行分区（suffle），然后在每个分区内进行排序

    val result: RDD[(String, String, Int)] = subjectAndTeacher
      //把参与排序的字段 放到key里
      .map(t => ((t._1._1, t._1._2, t._2), null))
      .repartitionAndSortWithinPartitions(myPartioner)
      .map(_._1)

    /**
     * (php,laowang,187)
     * (php,laoli,144)
     * (javaee,xiaoxu,360)
     * (bigdata,laozhao,539)
     * (bigdata,laosun,324)
     * (bigdata,laoduan,144)
     * (javaee,laojiang,300)
     * (php,laowu,131)
     * (javaee,laoyang,276)
     * (bigdata,laolong,115)
     * (bigdata,laozhang,24)
     */
    result.foreach(println(_))
    sc.stop()
  }
}


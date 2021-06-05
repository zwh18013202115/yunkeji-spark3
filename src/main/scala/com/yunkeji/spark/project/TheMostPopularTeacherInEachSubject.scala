package com.yunkeji.spark.project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * groupByKey的方法
 * 容易造成 某个分区数据量 分大 多个学科
 */
object TheMostPopularTeacherInEachSubject {
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

    val topN: Int = args(2).toInt
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

    /**
     * ((php,laowu),131)
     * ((javaee,xiaoxu),360)
     * ((javaee,laojiang),300)
     * ((bigdata,laozhang),24)
     * ((bigdata,laolong),115)
     * ((bigdata,laozhao),539)
     * ((bigdata,laosun),324)
     * ((javaee,laoyang),276)
     * ((php,laoli),144)
     * ((php,laowang),187)
     * ((bigdata,laoduan),144)
     */
    subjectAndTeacher.foreach(println(_))

    val sored: RDD[(String, (String, Int))] = subjectAndTeacher.map(t => {
      (t._1._1, (t._1._2, t._2))
      //groupByKey 容易造成 某个分区数据量 分大 多个学科
    }).groupByKey().flatMapValues(it => {
      //降序
      /**
       * 注意：这里的sortBy 是scala的排序 在内存中排 容易oom
       * 优化 可以使用 Treeset
       */
      it.toList.sortBy(t => -t._2).take(topN)
    })

    /**
     * (javaee,(xiaoxu,360))
     * (javaee,(laojiang,300))
     * (javaee,(laoyang,276))
     * (php,(laowang,187))
     * (php,(laoli,144))
     * (php,(laowu,131))
     * (bigdata,(laozhao,539))
     * (bigdata,(laosun,324))
     * (bigdata,(laoduan,144))
     */
    sored.foreach(println(_))


    sc.stop()
  }
}

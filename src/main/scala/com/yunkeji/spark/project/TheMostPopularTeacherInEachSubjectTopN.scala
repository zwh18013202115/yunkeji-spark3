package com.yunkeji.spark.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 看 打印
 * 多次触发 action
 * 分批 提交数据  这种方法在大数据 领域很实用
 * 利用 top() 来排序
 */
object TheMostPopularTeacherInEachSubjectTopN {
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
     * 优化：rdd的 sortBy()是在内存和 磁盘 排序的 不会内存溢出
     * 我们先过滤 出每个学科 再排序
     *
     * top()的排序规则 按顺序 （(String, String), Int） 如果数据很大 放到有效优先队列BoundedPriorityQueue  局部topN 到 全局topN
     * 我们自定义 规则 按 Int来排
     *
     * def top(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
     * takeOrdered(num)(ord.reverse)
     * }
     *
     * val mapRDDs = mapPartitions { items =>
     * // Priority keeps the largest elements, so let's reverse the ordering.
     * val queue = new BoundedPriorityQueue[T](num)(ord.reverse)
     * queue ++= collectionUtils.takeOrdered(items, num)(ord)
     * Iterator.single(queue)
     * }
     */

    //学科 可以根据 数据 distinct 算出来
    val subjects = Array("bigdata", "php", "javaee")
    for (subject <- subjects) {
      val bigdataRDD: RDD[((String, String), Int)] = subjectAndTeacher.filter(_._1._1.equals(subject))
      //定义一个排序规则 top本身降序
      implicit val orderRules: Ordering[((String, String), Int)] = Ordering[Int].on[((String, String), Int)](t => t._2)
      //bigdataRDD.top(3)(可以不用传) implicit
      /**
       * 看 打印
       * 多次触发 action
       * 分批 提交数据  这种方法在大数据 领域很实用
       */
      val res: Array[((String, String), Int)] = bigdataRDD.top(topN)

      /**
       * ((bigdata,laozhao),539)
       * ((bigdata,laosun),324)
       * ((bigdata,laoduan),144)
       *
       * ((php,laowang),187)
       * ((php,laoli),144)
       * ((php,laowu),131)
       *
       * ((javaee,xiaoxu),360)
       * ((javaee,laojiang),300)
       * ((javaee,laoyang),276)
       */
      res.foreach(println(_))
    }


    sc.stop()
  }
}

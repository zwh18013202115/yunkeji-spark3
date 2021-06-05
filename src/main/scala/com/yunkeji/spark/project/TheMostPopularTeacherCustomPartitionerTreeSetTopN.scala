package com.yunkeji.spark.project

import com.yunkeji.spark.partioner.SubjectPatitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet

/**
 * 自定义分区器
 */
object TheMostPopularTeacherCustomPartitionerTreeSetTopN {
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
     * 有时候：groupby 会造成 某个分区的 数据量 非常大 造成数据倾斜 如：多个学科  0分区的学科 比较多 1分区的 少
     * 这时候我们可以自定义分区：每个学科对应一个分区 partitionBy(自定义分区器)
     *
     */

    //这里先出发action 计算出subjects数量 就算1000万 学科 也不会 有问题
    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()

    //初始化分区
    val myPartioner = new SubjectPatitioner(subjects)
    //按照学科进行分区
    val partionBySubject: RDD[((String, String), Int)] = subjectAndTeacher.partitionBy(myPartioner)

    val result: RDD[((String, String), Int)] = partionBySubject.mapPartitions(it => {
      /**
       * 定义一个可以排序的特殊集合以及 排序规则
       * TreeSet本身是升序
       * 自定义降序 (t => -t._2)
       * 也可以用有限优先队列
       */
      val rules: Ordering[((String, String), Int)] = Ordering[Int].on[((String, String), Int)](t => -t._2)
      val sorter = new mutable.TreeSet[((String, String), Int)]()

      it.foreach(t => {
        //江数据添加到treeSet中
        sorter += t
        //把最后一个移除掉 永远保留tonN
        if (sorter.size > topN) sorter -= sorter.last
      })
      sorter.iterator
      // sorter.toIterator
    })

    result.foreach(println(_))
    sc.stop()
  }
}


/**
 * 自定义分区器
 */
/*
class MyPartioner(sub: Array[String]) extends Partitioner {

  //在主构造器中定义分区规则
  /**
   * 我们还可以 实现 Partitioner的 hashcode方法
   */
  private val nameToNum = new HashMap[String, Int]()
  var i = 0
  for (elem <- sub) {
    nameToNum(elem) = i
    i += 1
  }

  override def numPartitions: Int = {
    sub.length
  }

  //在Executor的Task中，suffle之前会调用
  override def getPartition(key: Any): Int = {
    val subject: String = key.asInstanceOf[(String, String)]._1
    nameToNum(subject) //根据学科名称获取对应的分区
  }
}*/

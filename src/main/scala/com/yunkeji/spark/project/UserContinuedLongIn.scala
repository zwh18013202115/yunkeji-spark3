package com.yunkeji.spark.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object UserContinuedLongIn {
  def main(args: Array[String]): Unit = {
    val isLocal: Boolean = args(0).toBoolean
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    if (isLocal) {
      conf.setMaster("local[4]")
    }

    val sc = new SparkContext(conf)
    //指定参数从哪里读取数据
    val rdd: RDD[String] = sc.textFile(args(1))

    val uidAndDate: RDD[(String, String)] = rdd.map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0), arr(1))
    })

    //将相同的 uid 进行分组
    val groupByKeyRdd: RDD[(String, Iterable[String])] = uidAndDate.groupByKey()

    //组内进行 排序
    val uidAndDateDiff: RDD[(String, (String, String))] = groupByKeyRdd.flatMapValues(it => {
      //将迭代器中的数据集 toList | toSet 去重 好一点| 数据量很大会oom
      val sortedRDD: List[String] = it.toSet.toList.sorted
      //定义一个日期工具类
      val calendar: Calendar = Calendar.getInstance()
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      var indext = 0

      sortedRDD.map(dateStr => {
        val date: Date = sdf.parse(dateStr)
        calendar.setTime(date)
        calendar.add(Calendar.DATE, -indext)
        indext += 1
        //当前时间 和 差的时间
        (dateStr, sdf.format(calendar.getTime))
      })
    })
    // uidAndDateDiff.foreach(println(_))


    val guidAndNumAndDate: RDD[(String, Int, String, String)] = uidAndDateDiff.map(t => {
      ((t._1, t._2._2), t._2._1)
    }).groupByKey().mapValues(it => {
      val list: List[String] = it.toList.sorted
      val size: Int = list.size
      val startTime: String = list.head
      val endTime: String = list.last
      (size, startTime, endTime)
    }).filter(t => t._2._1 >= 3).map(t => {
      (t._1._1, t._2._1, t._2._2, t._2._3)
    })

    /**
     * (guid01,3,2018-02-28,2018-03-02)
     * (guid02,3,2018-03-01,2018-03-03)
     * (guid01,4,2018-03-04,2018-03-07)
     */
    guidAndNumAndDate.foreach(println(_))
    sc.stop()

    //

  }
}

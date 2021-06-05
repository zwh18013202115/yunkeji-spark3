package com.yunkeji.spark.rdd

import com.yunkeji.spark.util.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import java.util.Date

object TreadSafeDemo {
  def main(args: Array[String]): Unit = {
    //val isLocal: Boolean = args(0).toBoolean
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    /* if (isLocal) {
       conf.setMaster("local[4]")
     }*/

    val sc = new SparkContext(conf)
    //指定参数从哪里读取数据
    //val rdd: RDD[String] = sc.textFile(args(1))
    val rdd: RDD[String] = sc.textFile("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\date.log")

    /*  rdd.map(line =>{
        val dateTime: Long = DateUtils.parse(line)
        dateTime
      }).foreach(println(_))*/
    rdd.mapPartitions(it => {
      //这样不会出现 线程安全问题 每个分区 对应一个task 初始化一次
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map(line => {
        val date: Date = sdf.parse(line)
        date.getTime
      })
    }).foreach(println(_))


    sc.stop()
  }
}

package com.yunkeji.spark.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import java.net.InetAddress

object SerializationDemo {
  def main(args: Array[String]): Unit = {
    val isLocal: Boolean = args(0).toBoolean
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    if (isLocal) {
      conf.setMaster("local[4]")
    }

    val sc = new SparkContext(conf)
    //指定参数从哪里读取数据
    val rdd: RDD[String] = sc.textFile(args(1))
    //val rulesMap: Map[String, String] = RulesMapObjectNotSer.rules
    rdd.map(t => {
      val rulesMap: Map[String, String] = RulesMapObjectNotSer.rules
      //闭包 excutor使用外部driver端的数据
      val pronvince: String = rulesMap.getOrElse(t, "未知")
      val taskId: Int = TaskContext.getPartitionId() //对应分区编号
      val threadId: Long = Thread.currentThread().getId //线程id
      val hostName: String = InetAddress.getLocalHost.getHostName//主机名
      (t, pronvince, taskId, threadId, hostName, rulesMap.toString())
    })

    /**
     * 一般的工具类 我们定义bject单例，在executor中只初始化一次，多个task共享  但是多线程例外 需要定义成class
     */
  }
}

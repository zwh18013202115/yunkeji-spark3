package com.yunkeji.spark.partioner

import org.apache.spark.Partitioner

import scala.collection.mutable.HashMap

class SubjectPartition2(sub: Array[String]) extends Partitioner {
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
    val subject = key.asInstanceOf[(String, String,Int)]._1
    nameToNum(subject) //根据学科名称获取对应的分区
  }
}

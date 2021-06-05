package com.yunkeji.spark.rdd

object RulesMapObjectNotSer extends Serializable { //drive才能  也可以定义在executor内部 频繁GC 但是不断地创建耗资源
  val rules = Map(
    "ln" -> "辽宁省",
    "bj" -> "北京市",
    "sh" -> "上海市",
    "sd" -> "山东省"
  )
}

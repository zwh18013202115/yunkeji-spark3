package com.yunkeji.spark.iterator

import scala.io.Source

object IteratorDemo {
  def main(args: Array[String]): Unit = {

    val lines: Iterator[String] = Source.fromFile("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\w.txt").getLines()

    val iteratorFilter: Iterator[String] = lines.filter(line => {
      println("filter")
      line.startsWith("h")
    })

    val iterator: Iterator[String] = iteratorFilter.map(line => {
      println("map")
      line.toUpperCase
    })

    println(iterator)

    /**
     * 迭代器 是最后一个 迭代器触发才执行 真正要数据才执行 UsersWhoHave3ConsecutiveDaysOrMore
     */

    iterator.foreach(it => {
      println(it)
    })

    /**
     * filter
     * filter
     * filter
     * non-empty iterator
     * map
     * HAOOP
     * filter
     */
  }
}

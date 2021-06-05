package com.yunkeji.spark.util

object MyModPartitioner {
  /**
   * 自定义key所在分区编号
   *
   * @param x   key的hashCode
   * @param mod 并行度 分区数
   * @return
   */
  def nonNegativeMod(x: Int, mod: Int) = {
    /**
     * rawMod有可能小于0
     */
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def main(args: Array[String]): Unit = {
    /**
     * flink -- 0
     * spark -- 1
     * flume -- 3
     *
     * hbase -- 1
     */
    val i: Int = nonNegativeMod("hbase".hashCode, 4)
    println(i)
  }
}

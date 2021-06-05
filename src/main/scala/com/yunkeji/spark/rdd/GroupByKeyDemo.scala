package com.yunkeji.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(
      List(
        "spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka",
        "hadoop", "flink", "hive", "flink"
      ), 4)


    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.map((_, 1)).groupByKey()
    groupByKeyRDD.collect().foreach(println(_))

    /**
     * ---groupByKey()
     * (hive,CompactBuffer(1, 1))
     * (flink,CompactBuffer(1, 1, 1))
     * (spark,CompactBuffer(1, 1, 1, 1))
     * (hadoop,CompactBuffer(1, 1))
     * (hbase,CompactBuffer(1))
     * (kafka,CompactBuffer(1, 1, 1, 1))
     */
    //Thread.sleep(1000000)
    rdd.map((_, 1)).groupBy(_._1).foreach(println(_))

    /**
     * -- groupBy
     * (kafka,CompactBuffer((kafka,1), (kafka,1), (kafka,1), (kafka,1)))
     * (hive,CompactBuffer((hive,1), (hive,1)))
     * (flink,CompactBuffer((flink,1), (flink,1), (flink,1)))
     * (spark,CompactBuffer((spark,1), (spark,1), (spark,1), (spark,1)))
     * (hadoop,CompactBuffer((hadoop,1), (hadoop,1)))
     * (hbase,CompactBuffer((hbase,1)))
     */
    sc.stop()
  }
}

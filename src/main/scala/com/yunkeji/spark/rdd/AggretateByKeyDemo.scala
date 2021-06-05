package com.yunkeji.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object AggretateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val rdd: RDD[String] = sc.parallelize(
      List(
        "spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka",
        "hadoop", "flink", "hive", "flink"
      ), 4)


    val wordOneRDD: RDD[(String, Int)] = rdd.map((_, 1))

    // 传两个 函数  局部聚合 全局聚合
    val result: RDD[(String, Int)] = wordOneRDD.aggregateByKey(0)(_ + _, _ + _)

    //TODO 1.通过 API 的方式转换(了解) rdd --> df
    // 映射出来一个 RDD[Row], 因为 DataFrame其实就是 DataSet[Row]
    //val rddRow: RDD[Row] = result.map(x => Row(x._1, x._2))
    // 创建 StructType 类型
    //val types = StructType(Array(StructField("subject", StringType), StructField("num", IntegerType)))
    //val res: DataFrame = spark.createDataFrame(rddRow, types)
    //TODO 2.通过样例类反射转换(最常用)
    import spark.implicits._
    val resRDD: RDD[Sub] = result.map(e => Sub(e._1, e._2))
    resRDD.toDF.write.csv("dataSpark/aggregate-out")


    sc.stop()

  }
}

case class Sub (
               var subject: String,
               var num: Int
              )
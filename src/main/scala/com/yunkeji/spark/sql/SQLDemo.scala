package com.yunkeji.spark.sql

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.control.Exception.noCatch.desc

object SQLDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    /**
     * Dataset是RDD的进一步封装，里面持有rdd，有更多的schema信息，可以根据schema生成执行计划
     * 是一种强类型语言 还没开始执行就能知道 封装什么信息  rdd不是
     * 是一种逻辑计划 会生成执行计划 explain
     */

    val dataSet: Dataset[String] = spark.read.textFile("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\word.txt")

    /**
     * 导入隐士转换：
     * 1.object 单例的 直接导入
     * 2.如果是class的话需要 class的实例 如 spark是SparkSession的实例--import spark.implicits._
     */
    import spark.implicits._
    val wordsDS: Dataset[String] = dataSet.flatMap(_.split(" "))

    /**
     * root
     * |-- value: string (nullable = true)
     */
    //wordsDS.printSchema()

    /**
     * +----------+
     * |     value|
     * +----------+
     * |     spark|
     * |     fluem|
     * |     flink|
     * |    hadoop|
     * |     kafka|
     * |       add|
     * |clickhouse|
     */
    // wordsDS.show(10)
    /**
     * 两种方法：1。SQL 2.DSL方式
     */

    //注册 视图
    //wordsDS.createTempView("tmp")
    //执行sql
    //spark.sql("SELECT value,count(*) AS counts FROM tmp GROUP BY value ORDER BY counts DESC").show()

    //2.DSL方式 导入fuctitions函数

    import org.apache.spark.sql.functions._
    wordsDS.select("value")
      .groupBy($"value") //可以不加$
      .agg(count($"value") as "counts")
      //.orderBy($"counts".desc) //必须加$
      .orderBy($"counts" desc) //必须加$
      .show()
    spark.close()
  }
}

package com.yunkeji.spark.project

import com.yunkeji.spark.bean.DwGuidHistory
import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

object UsersHave3ConsecutiveDaysOrMore {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val dataDS = spark.read.textFile("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\comsumer.txt")

    val df: DataFrame = dataDS.map(line => {
      val arr: Array[String] = line.split(",")
      //      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      //      val dateStr: Date = sdf.parse(arr(1))
      DwGuidHistory(arr(0), arr(1))
    }).toDF("guid", "dateStr")

    df.createTempView("tmp")

    spark.sql(
      """
        |select
        |guid,
        |dateStr,
        |lead(dateStr,2,"1970-01-01") over(partition by guid order by dateStr) as date_to2
        |from tmp
        |""".stripMargin).createTempView("tmp3")

    spark.sql(
      """
        |select
        |guid,
        |dateStr,
        |date_to2,
        |datediff(date_to2, dateStr) as diff
        |from tmp3
        |""".stripMargin).createTempView("tmp4")


    spark.sql(
      """
        |select
        |guid,
        |dateStr,
        |date_to2,
        |diff
        |from tmp4
        |where diff >=3
        |""".stripMargin).show(20)


    /**
     * +------+----------+----------+----+
     * |  guid|   dateStr|  date_to2|diff|
     * +------+----------+----------+----+
     * |guid02|2018-03-02|2018-03-06|   4|
     * |guid01|2018-03-01|2018-03-04|   3|
     * |guid01|2018-03-02|2018-03-05|   3|
     * +------+----------+----------+----+
     */
    spark.stop()
  }
}

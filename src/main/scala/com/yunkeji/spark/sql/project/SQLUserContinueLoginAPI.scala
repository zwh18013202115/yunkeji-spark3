package com.yunkeji.spark.sql.project

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLUserContinueLoginAPI {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    val df: DataFrame = spark
      .read
      .option("header", true) //有表头 会推断类型
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\user_login.csv")

    /**
     * 想要使用spark内置的函数 必须导包import org.apache.spark.sql.functions._
     * 使用$和'-->是字段变成col 必须导隐式转换
     * selectExpr方法本质与select方法中使用expr函数是一样的，都是用来构建复杂的表达式
     * df.select(expr("guid as uid")).show()
     * df.selectExpr("appid as newappid").show()
     */
    import org.apache.spark.sql.functions._
    import spark.implicits._
    df.select(
      $"guid",
      'date_day,
      row_number() over(Window.partitionBy($"guid").orderBy($"date_day".asc)) as("rn")
    ).select(
      'guid,
      'date_day,
      expr("date_sub(date_day,rn) as date_dif") //sql的片段 表达式
      //date_sub('date_day,'rn).as('date_dif)
    ).groupBy(
      "guid",
      "date_dif"
    ).agg(count('*).as("times"),min('date_day).as("start_date"),max('date_day).as("end_date"))
      .select(
        'guid,
        'times,
        'start_date,
        'end_date
      ).filter('times >=3).show()

    /**
     * +------+-----+----------+----------+
     * |  guid|times|start_date|  end_date|
     * +------+-----+----------+----------+
     * |guid02|    3|2018-03-01|2018-03-03|
     * |guid01|    3|2018-02-28|2018-03-02|
     * |guid01|    4|2018-03-04|2018-03-07|
     * +------+-----+----------+----------+
     */

    spark.close()
  }
}

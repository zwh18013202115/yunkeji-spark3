package com.yunkeji.spark.sql.project

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLUserContinueLogin {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    val df: DataFrame = spark
      .read
      .option("header", true) //有表头 会推断类型
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\user_login.csv")
    //  df.printSchema()
    //   df.show()

    df.createTempView("tmp")

    spark.sql(
      """
        |select
        |		guid,
        |		date_day,
        |	date_sub(date_day,rn) date_dif
        |	from (
        |		select
        |			guid,
        |			date_day,
        |		row_number() over(partition by guid order by date_day asc ) rn
        |		from tmp
        |	)t1
        |""".stripMargin).createTempView("tmp2")

    spark.sql(
      """
        |select
        |	  guid,
        |	  count(*) times,
        |	  min(date_day) start_date,
        |	  max(date_day) end_date
        |from tmp2 group by guid,date_dif having times >=3
        |""".stripMargin).show()

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

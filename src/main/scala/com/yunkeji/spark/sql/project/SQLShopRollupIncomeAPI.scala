package com.yunkeji.spark.sql.project

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, UnboundedPreceding}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLShopRollupIncomeAPI {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    val df: DataFrame = spark
      .read
      .option("header", true) //有表头 会推断类型
      .option("inferSchema", true)
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\shop_day.csv")

    /**
     * 需求：统计店铺按照月份的销售额和累计到该月的总销售额
     */
    import org.apache.spark.sql.functions._
    import spark.implicits._
    df.select(
      'sid,
      date_format('dt, "yyyy-MM").as("mth"),
      'money
    ).groupBy(
      'sid,
      'mth)
      .agg(sum("money").as("mth_sales"))
      //.sum("money").as("mth_sales") //会报错,必须放在agg()
      .select(
        'sid,
        'mth,
        'mth_sales,
        sum($"mth_sales") over(Window.partitionBy("sid").orderBy("mth").rowsBetween(Window.unboundedPreceding,Window.currentRow)) as("total_sales")
        //expr("sum(mth_sales) over(partition by sid order by mth rows between unbounded preceding and current row ) as total_sales")
      ).show()


    spark.close()
  }
}

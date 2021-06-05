package com.yunkeji.spark.sql.project

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLRollupFlow {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    val df: DataFrame = spark
      .read
      .option("header", true) //有表头 会推断类型
      .option("inferSchema", true)
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\v_flow.csv")

    df.createTempView("v_flow")

    spark.sql(
      """
        |select
        |	id,
        |	min(begin_time) begin_time,
        |	max(end_time) end_time,
        |	sum(down_flow) total_down_flow
        |from
        | (
        |	select
        |	id,
        |	begin_time,
        |	end_time,
        |	down_flow,
        |	sum(flag) over(partition by id order by begin_time asc rows between unbounded preceding and current row) fid
        |from
        |(
        |	select
        |		id,
        |		begin_time,
        |		end_time,
        |		down_flow,
        |		if((unix_timestamp(begin_time)- unix_timestamp(lag_time))/60 >10,1,0 ) flag
        |	from
        | (
        |	SELECT
        |		id,
        |		begin_time,
        |		LAG(end_time,1,begin_time) over(PARTITION BY id  ORDER BY begin_time) lag_time
        |		end_time,
        |		down_flow
        |	FROM v_flow
        |		)t1
        |	)t2
        |)t3 group by id,fid
        |""".stripMargin).show()


    spark.close()
  }
}

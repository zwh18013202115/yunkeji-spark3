package com.yunkeji.spark.sql

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CreateDataFramOrc {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    val dataFrameJson: DataFrame = spark.read.json("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\user.json")

    dataFrameJson.printSchema()


    dataFrameJson.show()
    //两种方式 过滤
    import spark.implicits._
    dataFrameJson.filter(row =>row.getString(0) == null)
    val df: DataFrame = dataFrameJson.filter($"_corrupt_record".isNull).select("name", "age", "fw", "gender")

    /**
     * SaveMode.ErrorIfExists 默认 有就会报错
     * SaveMode.Ignore 有就不写 不报错
     * SaveMode.Append 追加
     * SaveMode.Overwrite 有就覆盖
     *
     */
    df.coalesce(1).write.mode(SaveMode.Overwrite).orc("F:/data/Spark3/orc/user")

    //spark.read.format("json").load("F:/data/Spark3/json/user").show()
    spark.read.orc("F:/data/Spark3/json/user").show()
    spark.close()
  }
}

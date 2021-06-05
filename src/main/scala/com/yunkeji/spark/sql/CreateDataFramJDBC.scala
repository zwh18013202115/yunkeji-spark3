package com.yunkeji.spark.sql

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CreateDataFramJDBC {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    val dataFrameJson: DataFrame = spark.read.json("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\user.json")

    /**
     * root
     * |-- _corrupt_record: string (nullable = true) --脏数据 不是json的
     * |-- age: long (nullable = true)
     * |-- fw: double (nullable = true)
     * |-- gender: string (nullable = true)
     * |-- name: string (nullable = true)
     * 会打印所有可能出现的列 ------>>>注意：json会读取所有行 因为每一行大的字段 有可能不一样
     */
    dataFrameJson.printSchema()

    /**
     * +--------------------+----+----+------+--------+
     * |     _corrupt_record| age|  fw|gender|    name|
     * +--------------------+----+----+------+--------+
     * |                null|  33|99.0|  null| laoduan|
     * |                null|  23|99.8|  null| laozhao|
     * |                null|  30|99.3|  null|laozhang|
     * |{"name":"laozhang...|null|null|  null|    null|
     * |                null|null|99.5|     F| laowang|
     * |                null|null|99.2|     M| laoyang|
     * +--------------------+----+----+------+--------+
     */
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
    df.coalesce(1).write.mode(SaveMode.Overwrite).json("F:/data/Spark3/json/user")


    //spark.read.format("json").load("F:/data/Spark3/json/user").show()
    spark.read.json("F:/data/Spark3/json/user").show()
    spark.close()
  }
}

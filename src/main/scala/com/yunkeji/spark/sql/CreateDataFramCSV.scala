package com.yunkeji.spark.sql

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CreateDataFramCSV {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

 /*   val dataFrameCsv: DataFrame = spark.read
      .option("inferschema", true) //推断数据类型
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\user.csv") //没有表头header
*/


   /* val schema: StructType = new StructType()
      .add("name", DataTypes.StringType)
      .add("age", DataTypes.IntegerType)
      .add("fw", DataTypes.DoubleType)
    val dataFrameCsv: DataFrame = spark.read
      .schema(schema)
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\user.csv") //没有表头header*/

    //读取一行
    //dataFrameCsv.toDF("name","age","fw").printSchema()

    val dataFrameCsv: DataFrame = spark.read
      .option("header", true) //有表头 不会推断类型 类型全为string
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\user.csv") //有表头header




    //两种方式 过滤
    import spark.implicits._
    /**
     * SaveMode.ErrorIfExists 默认 有就会报错
     * SaveMode.Ignore 有就不写 不报错
     * SaveMode.Append 追加
     * SaveMode.Overwrite 有就覆盖
     *
     */
    Thread.sleep(10000000)
    spark.close()
  }
}

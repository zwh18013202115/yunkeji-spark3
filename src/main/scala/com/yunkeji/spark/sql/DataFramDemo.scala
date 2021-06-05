package com.yunkeji.spark.sql

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.SparkSession

import scala.beans.BeanProperty

object DataFramDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)

    //TODO 1.通过 API 的方式转换(了解) rdd --> df
    // 映射出来一个 RDD[Row], 因为 DataFrame其实就是 DataSet[Row]
    //val rddRow: RDD[Row] = result.map(x => Row(x._1, x._2))
    // 创建 StructType 类型
    //val types = StructType(Array(StructField("subject", StringType), StructField("num", IntegerType)))
    //val res: DataFrame = spark.createDataFrame(rddRow, types)
    //TODO 2.通过样例类反射转换(最常用)
    import spark.implicits._

    //TODO 3.通过普通的类
    //val res: DataFrame = spark.createDataFrame(rddRow, classOf[Person])
    // TODO 4.通过java的类 只要生成getter就可以获得数据了 不需要生成setter
    //val res: DataFrame = spark.createDataFrame(rddRow, classOf[Person])

    //TODO 5.通过 元组 + import spark.implicits._ toDF("name","age","fw")

    //TODO 6.通过 ROW +  StructType 类型


    //TODO 7.如果数据本身带 schema 类型  JSON JDBC ORC Parquet Csv





  }
}

/**
 * 普通大的类没有getter setter 方法
 * 需要加 val var 并且 前面加@BeanProperty
 *
 * @param name
 * @param age
 * @param fw
 */
class Person(
              @BeanProperty val name: String,
              @BeanProperty var age: Int,
              @BeanProperty var fw: Double
            )
package com.yunkeji.spark.sql

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

object UDAFDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)
    import spark.implicits._
    val df: DataFrame = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\salary.csv")

    df.createTempView("v_emp")
    spark.sql("select dept, avg(salary) avg_salary from v_emp group by dept ").show()

    //    spark.udf.register("my_avg",new MyAVGDefindFunction)
    //    spark.sql("select dept, my_avg(salary) avg_salary from v_emp group by dept ").show()

    import org.apache.spark.sql.functions._
    val avgAgg = new Aggregator[Double, (Double, Int), Double] {
      //初始值类型
      override def zero: (Double, Int) = (0.0, 0)

      //分区内 --局部聚合
      override def reduce(b: (Double, Int), a: Double): (Double, Int) = {
        (b._1 + a, b._2 + 1)
      }

      //全局聚合
      override def merge(b1: (Double, Int), b2: (Double, Int)): (Double, Int) = {
        (b1._1 + b2._1, b1._2 + b2._2)
      }

      //最终返回结果
      override def finish(reduction: (Double, Int)): Double = {
        reduction._1 / reduction._2
      }

      /**
       * 如果是对象 Encoders.bean
       *
       */
      //中间结果的 编码
      override def bufferEncoder: Encoder[(Double, Int)] = {
        Encoders.tuple(Encoders.scalaDouble, Encoders.scalaInt)
      }

      //最终返回结果的编码
      override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }
    spark.udf.register("my_avg", udaf(avgAgg))
    spark.sql("select dept, my_avg(salary) avg_salary from v_emp group by dept ").show()

    /**
     * 算术平均数：上面算的是
     * 几何平均数：根号下 数值几个 就几次方
     *     用途：计算平均发展速度  正态分布 等
     */


    spark.stop()
  }
}

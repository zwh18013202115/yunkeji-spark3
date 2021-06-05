package com.yunkeji.spark.sql

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

import java.lang

/**
 * 算术平均数：上面算的是
 * 几何平均数：根号下 数值几个 就几次方
 * 用途：计算平均发展速度  正态分布 等
 */
object UDFJIHEAVGDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)
    import spark.implicits._

    val nums: Dataset[lang.Long] = spark.range(1, 10) //包头 不包尾
    nums.createTempView("v_nums")

    /**
     * +---+
     * | id|
     * +---+
     * |  1|
     */
    import org.apache.spark.sql.functions._
    val agg = new Aggregator[Long, (Long, Int), Double] {
      override def zero: (Long, Int) = (1L, 0)

      override def reduce(b: (Long, Int), a: Long): (Long, Int) = (b._1 * a, b._2 + 1)

      override def merge(b1: (Long, Int), b2: (Long, Int)): (Long, Int) = (b1._1 * b2._1, b1._2 + b2._2)

      override def finish(reduction: (Long, Int)): Double = Math.pow(reduction._1.toDouble, 1 / reduction._2.toDouble)

      override def bufferEncoder: Encoder[(Long, Int)] = Encoders.tuple(Encoders.scalaLong, Encoders.scalaInt)

      override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }
    spark.udf.register("geo_mean", udaf(agg))

    spark.sql(
      """
        |SELECT geo_mean(id) FROM v_nums
        |""".stripMargin).show()

    /**
     * +-----------------+
     * |       anon$1(id)|
     * +-----------------+
     * |4.147166274396913|
     * +-----------------+
     */
    spark.stop()
  }
}

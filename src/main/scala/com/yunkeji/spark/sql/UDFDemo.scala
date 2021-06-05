package com.yunkeji.spark.sql

import com.alibaba.fastjson.util.IOUtils
import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object UDFDemo {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)
    import spark.implicits._
    val list = List(("贵州省", "贵阳市", "花果园"), ("北京市", "北京市", "朝阳区"), ("上海市", "上海市", "浦东新区"))
    val ds: Dataset[(String, String, String)] = spark.createDataset(list)
    val df: DataFrame = ds.toDF("province", "city", "district")
    df.createTempView("v_location")

    //TODO 1.用sql自带的函数
    spark.sql("select concat_ws('|',province,city,district) as location from v_location ").show()

    /**
     * concat('|',province,city,district)函数
     * +---------------------+
     * |             location|
     * +---------------------+
     * |  |贵州省贵阳市花果园|
     * |  |北京市北京市朝阳区|
     * ||上海市上海市浦东新区|
     * +---------------------+
     *
     * concat_ws('|',province,city,district)
     * +----------------------+
     * |              location|
     * +----------------------+
     * |  贵州省|贵阳市|花果园|
     * |  北京市|北京市|朝阳区|
     * |上海市|上海市|浦东新区|
     * +----------------------+
     */
    //TODO 2.自定义udf  scala中的函数
    val func = (split: String, province: String, city: String, district: String) => province + split + city + split + district

    //（改造）最后一个可变参数 不行
  /*  val func = (split: String, p1: String, p2: String, params: String*) => {
      var res = p1 + split + p2
      if (params.nonEmpty) { //如果不为空
        res += split + params.toArray.mkString(split)
      } else {
        res
      }
    }*/

    spark.udf.register("MY_CONCAT_WS", func)
    spark.sql("select MY_CONCAT_WS('|',province,city,district) as location from v_location ").show()
    //TODO 3.api 方式

    import org.apache.spark.sql.functions._
    df.select(
      expr("MY_CONCAT_WS('|',province,city,district) as location")
    ).show()

    df.select()
    spark.stop()
  }
}

package com.yunkeji.spark

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.yunkeji.spark.bean.OrderBean
import com.yunkeji.spark.util.SparkUtil
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

object LocationIncome {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)
    //val lines: RDD[String] = sparkSession.sparkContext.textFile(args(0))
    val lines: RDD[String] = sparkSession.sparkContext.textFile("F:\\CodeData\\study\\yunkeji-spark3\\src\\main\\resources\\order.log")

    //TODO 1.江数据转成Json对象  解析json时会出现异常程序会挂掉 所以要捕获异常
    val orderRDD: RDD[OrderBean] = lines.map(line => {
      var orderBean: OrderBean = null
      try {
        //解析成功
        orderBean = JSON.parseObject(line, classOf[OrderBean])
      } catch {
        //解析失败
        case e: JSONException => e.printStackTrace()
      }
      //最终返回
      orderBean
    })

    //过滤
    val filterOrderRDD: RDD[OrderBean] = orderRDD.filter(_ != null)

    //关联
    val beanRDD: RDD[OrderBean] = filterOrderRDD.mapPartitions(iter => {
      //创建连接
      val httpClient: CloseableHttpClient = HttpClients.createDefault()

      //迭代分区中的每个数据
      val iterator: Iterator[OrderBean] = iter.map(bean => {
        val longitude: Double = bean.longitude
        val latitude: Double = bean.latitude
        //异步请求高德地图的地址
        val url: String = "https://restapi.amap.com/v3/geocode/regeo";
        //请求高德地图的秘钥，注册高德地图开发者后获得
        val key: String = "4924f7ef5c86a278f5500851541cdcff";

        val httpGet = new HttpGet(url + "?location=" + longitude + "," + latitude + "&key=" + key)
        val response: CloseableHttpResponse = httpClient.execute(httpGet)

        try {
          //解析成功
          val entity: HttpEntity = response.getEntity
          var province: String = null
          var city: String = null
          if (response.getStatusLine.getStatusCode == 200) { //解析返回的结果，获取省份、城市等信息
            val result: String = EntityUtils.toString(entity)
            val jsonObj: JSONObject = JSON.parseObject(result)
            val regeocode: JSONObject = jsonObj.getJSONObject("regeocode")
            if (regeocode != null && !regeocode.isEmpty) {
              val address: JSONObject = regeocode.getJSONObject("addressComponent")
              province = address.getString("province")
              city = address.getString("city")
            }
          }
          bean.province = province //将返回的结果给省份赋值
          bean.city = city //将返回的结果给城市赋值

        } catch {
          //关联失败
          case e: JSONException => e.printStackTrace()
        } finally {
          response.close()
        }
        //判断处理最后一条数据后关闭
        if (!iter.hasNext){
          httpClient.close()
        }
        bean
      })

      /**
       * 在这里关闭连接会报错
       *  原因：因为真正执行map方法的逻辑的是触发action的时候执行
       *  还没来得及处理连接就被关闭了
       *
       *  解决：判断处理最后一个迭代器后没有数据 关闭
       *
       */
     // httpClient.close()
      iterator
    })
    val result: RDD[(String, Double)] = beanRDD.map(bean => (bean.province, bean.money)).reduceByKey(_ + _)

    val r: Array[(String, Double)] = result.collect()

    println(r.toBuffer)

    sparkSession.sparkContext.stop()
  }
}

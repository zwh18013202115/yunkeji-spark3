package com.yunkeji.spark.streaming

import com.yunkeji.spark.streaming.util.JedisConnectionPool
import redis.clients.jedis.{Jedis, Pipeline, Response}

object JedisTest {
  def main(args: Array[String]): Unit = {

   // val jedis = new Jedis("hadoop101", 9379) //会连接超时
    val jedis: Jedis = JedisConnectionPool.getConnection()
   // jedis.set("string-key","666")

    //开启事务 piepeline 单机 主从 支持
    var pipeline: Pipeline = null
    try {
      pipeline = jedis.pipelined()
      //使用第15个库 总的可以有16个
     // pipeline.select(0)
      //开启等多个操作
      pipeline.multi()

      pipeline.set("key3", "411")
     // val i = 1/0
      pipeline.set("key4", "311")
      //提交事务
      pipeline.sync()
      pipeline.exec()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        //失败取消
        pipeline.discard()
    } finally {
      if (pipeline != null) pipeline.close()
      if (jedis != null) jedis.close()
    }

  }
}

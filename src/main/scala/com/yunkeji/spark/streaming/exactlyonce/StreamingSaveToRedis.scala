package com.yunkeji.spark.streaming.exactlyonce

import com.yunkeji.spark.streaming.util.{DruidConnectionPool, JedisConnectionPool, MyPropertiesUtil, OffsetUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.lang
import java.sql.{Connection, PreparedStatement}
import java.util.Properties

object StreamingSaveToRedis {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  val topic = "test_topic"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(args(0)).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    val appId = args(0)
    val groupId = args(1)

    // kafka消费者配置
    var kafkaParam: scala.collection.mutable.Map[String, Object] = collection.mutable.Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list, //用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      //latest自动重置偏移量为最新的偏移量
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
      //如果是false，会需要手动维护kafka偏移量
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
    )

    val offsets: collection.Map[TopicPartition, Long] = OffsetUtils.queryHistoryOffsetFromRedis(appId, groupId)
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets)
    )


    /**
     * foreachRDD既不是transformation 也不是action算子
     * 它会周期性调用(传进来的函数rdd 会被周期性调用) ，传进来的函数实在driver端调用的
     */
    kafkaDS.foreachRDD(rdd => {
      //在driver端处理  先判断 不为空 否则每个批次都要提交 浪费资源
      if (!rdd.isEmpty()) {
        //获取偏移量 只有kafkaRDD有偏移量 只有第一手的rdd可以获取偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //executor到端
        val rddBykey: RDD[(String, Int)] = rdd.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        //将计算好的结果数据收集到river 适合聚合类的情况
        val result: Array[(String, Int)] = rddBykey.collect()


        val jedis = JedisConnectionPool.getConnection()
        //开启事务 piepeline 单机 主从 支持
        var pipeline: Pipeline = null
        try {
          pipeline = jedis.pipelined()
          pipeline.select(15)
          //使用第15个库 总的可以有16个
          pipeline.select(0)
          //开启等多个操作
          pipeline.multi()
          for (elem <- result) {
            //有就插入 没有就更新 hash upsert
            pipeline.hincrBy("WORDCOUNT", elem._1, elem._2)
          }
          //提交偏移量
          for (ranges <- offsetRanges) {
            val topic1: String = ranges.topic
            val partition: Int = ranges.partition
            val offset: Long = ranges.untilOffset
            pipeline.hset(appId + "_" + groupId, topic1 + "_" + partition, offset.toString)
          }

          //提交事务
          pipeline.sync()
          pipeline.exec()
        } catch {
          case e: Exception =>
            e.printStackTrace()
            //失败取消
            pipeline.discard()
            ssc.stop(true) //优雅停止当前任务
        } finally {
          if (pipeline != null) pipeline.close()
          if (jedis != null) jedis.close()
        }

      }

    })

    //开启
    ssc.start()
    //让程序一直在driver挂起运行
    ssc.awaitTermination()
  }
}

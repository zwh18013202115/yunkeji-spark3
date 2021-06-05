package com.yunkeji.spark.streaming.util

import java.{io, lang}
import java.util.Properties

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author : zhao
 * @creat : 2021-03-07  20:16
 * @description: 读取kafka的工具类
 *
 */
object MyKafkaUtil {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")

  // kafka消费者配置
  var kafkaParam: scala.collection.mutable.Map[String, Object] = collection.mutable.Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list, //用于初始化链接到集群的地址
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    ConsumerConfig.GROUP_ID_CONFIG -> "gmall0523_group",
    //latest自动重置偏移量为最新的偏移量
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
  )

  // 创建DStream，返回接收到的输入数据   使用默认的消费者组
  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    dStream
  }

  //在对Kafka数据进行消费的时候，指定消费者组
  def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam(ConsumerConfig.GROUP_ID_CONFIG) = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位置策略
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)) //消费策略
    dStream
  }

  //从指定的偏移量位置读取数据
  def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String)
  : InputDStream[ConsumerRecord[String, String]] = {
    kafkaParam("group.id") = groupId
    val dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
    dStream
  }


}

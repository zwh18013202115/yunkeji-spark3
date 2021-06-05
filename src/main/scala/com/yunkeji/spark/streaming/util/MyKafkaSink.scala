package com.yunkeji.spark.streaming.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties

/**
 * @author : zhao
 * @creat : 2021-03-09  14:55
 * @description: 向kafka主题发送数据
 *
 */
object MyKafkaSink {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaProducer: KafkaProducer[String, String] = null
  val key_StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"
  val value_StringSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  def createKafkaProducer: KafkaProducer[String, String] = {
    val properties = new Properties
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, key_StringSerializer)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value_StringSerializer)
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, (true: java.lang.Boolean)) //开启幂等性机制

    var producer: KafkaProducer[String, String] = null
    try
      producer = new KafkaProducer[String, String](properties)
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer
  }

  def send(topic: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, msg))
  }

  def send(topic: String, key: String, msg: String): Unit = {
    if (kafkaProducer == null) kafkaProducer = createKafkaProducer
    kafkaProducer.send(new ProducerRecord[String, String](topic, key, msg))
  }

}

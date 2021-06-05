package com.yunkeji.spark.streaming

import com.yunkeji.spark.streaming.util.MyKafkaUtil.kafkaParam
import com.yunkeji.spark.streaming.util.MyPropertiesUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang
import java.util.Properties

/**
 * 聚合类 + 事务 实现 exactlyOne
 * 非聚合类 + 幂等性写入 实现 exactlyOne
 */
object StreamingDemo {

  private val properties: Properties = MyPropertiesUtil.load("config.properties")
  val broker_list = properties.getProperty("kafka.broker.list")
  val topic = "test_topic"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")

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

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
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
        rdd.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)
        //异步提交偏移量
        kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }

    })

    //开启
    ssc.start()
    //让程序一直在driver挂起运行
    ssc.awaitTermination()
  }
}

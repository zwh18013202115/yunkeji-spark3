package com.yunkeji.spark.streaming.exactlyonce

import com.yunkeji.spark.streaming.StreamingDemo.topic
import com.yunkeji.spark.streaming.util.{DruidConnectionPool, MyPropertiesUtil, OffsetUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import java.lang
import java.sql.{Connection, PreparedStatement}
import java.util.Properties

object StreamingSaveToMysql {
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

    val offsets:collection.Map[TopicPartition, Long] = OffsetUtils.queryHistoryOffsetFromMySql(appId,groupId)
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam,offsets)
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

        var connection: Connection = null
        var pstm: PreparedStatement = null
        var pstm2: PreparedStatement = null
        try {
          //获取数据库连接池
          connection = DruidConnectionPool.getConnection
          //关闭自动提交事务 开启事务
          connection.setAutoCommit(false)
          //先用？？占位防止注入
          //将结果数据写道mysql t_wordcount 使用 有就更新 没有就插入
          val sql = "INSERT INTO t_wordcount (word, counts) VALUES (?, ?) ON DUPLICATE KEY UPDATE counts = counts + ?"
          pstm = connection.prepareStatement(sql)
          for (t <- result) {
            pstm.setString(1, t._1)
            pstm.setLong(2, t._2) //插入
            pstm.setLong(3, t._2) //更新的
            pstm.executeUpdate()
            //pstm.addBatch()
          }
          //pstm.executeBatch()

          //提交偏移量到MySQL t_kafka_offset
          //设计 app_id-消费者组 ,topic_partition-主题-分区, offset-结束偏移量 ;app_id和topic_partition设计成联合主键
          val sql_offset = "INSERT INTO t_kafka_offset(app_id, topic_partition,offset) VALUES (?, ?,?) ON DUPLICATE KEY UPDATE offset = ?"
          pstm2 = connection.prepareStatement(sql_offset)
          for (range <- offsetRanges) {
            val topic: String = range.topic
            val partition: Int = range.partition
            val untilOffset: Long = range.untilOffset //拿到哪里的偏移量 就行 起始偏移量不用拿

            pstm2.setString(1, appId + "_" + groupId)
            pstm2.setString(2, topic + "_" + partition)
            pstm2.setLong(3, untilOffset)
            pstm2.setLong(4, untilOffset) //更新
            pstm2.executeUpdate()
          }

          //提交事务
          connection.commit()
        } catch {
          case e: Exception => e.printStackTrace()
            //回滚
            connection.rollback()
            //重启 spark没有  优雅停止
            ssc.stop(true)
          //写shell脚本 重启
        } finally {
          if (pstm != null) {
            pstm.close()
          }
          if (pstm2 != null) {
            pstm2.close()
          }
          if (connection != null) {
            connection.close()
          }
        }

      }

    })

    //开启
    ssc.start()
    //让程序一直在driver挂起运行
    ssc.awaitTermination()
  }
}

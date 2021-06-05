/*package com.yunkeji.spark.streaming.exactlyonce

import com.alibaba.fastjson.{JSON, JSONException}
import com.yunkeji.spark.streaming.StreamingDemo.topic
import com.yunkeji.spark.streaming.bean.Order
import com.yunkeji.spark.streaming.util.{HbaseUtil, JedisConnectionPool, OffsetUtils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}


import java.{lang, util}*/

/**
 * create view “t_orders” (pk VARCHAR PRIMARY KEY,"offset"."appid_groupid" VARCHAR,"offset"."offset" UNSIGNED_LONG);
 * select topic_partition, max("offset") from "t_orders" where "appid_groupid" = 'gid' group by "topic_partition"
 */
object KafkaToHbase {
  def main(args: Array[String]): Unit = {
   /* //true a1 g1 ta,tb
    val Array(isLocal, appName, groupId, allTopics) = args
    val conf: SparkConf = new SparkConf().setAppName(appName)
    if (isLocal.toBoolean) conf.setMaster("local[5]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(3))
    val topics: Array[String] = allTopics.split(",")
    // kafka消费者配置
    var kafkaParam: scala.collection.mutable.Map[String, Object] = collection.mutable.Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> topics, //用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      //如果没有从最开始的读 earliest 有就接着偏移量读
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
      //如果是false，会需要手动维护kafka偏移量
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
    )

    val offsets = OffsetUtils.queryHistoryOffsetFromHbase(appName,groupId)

    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParam,offsets)
    )

    kafkaDS.foreachRDD(rdd => {

      //在driver端处理  先判断 不为空 否则每个批次都要提交 浪费资源
      if (!rdd.isEmpty()) {
        //获取偏移量 只有kafkaRDD有偏移量 只有第一手的rdd可以获取偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //executor到端
        val lines  = rdd.map(_.value())
        val orderDS: RDD[Order] = lines.map(line => {
          var order: Order = null
          //解析数据 捕获异常
          try {
            order = JSON.parseObject(line, classOf[Order])
          } catch {
            case e: JSONException =>
          } finally {}
          order
        })
        //过滤数据
        val fitered: RDD[Order] = orderDS.filter(_ != null)

        //关键
        fitered.foreachPartition(iter => {

          val connection: Connection = HbaseUtil.getConnection("hadoop101", 2181)
          //使用闭包 获取当前Task的partitionId，然后到offsetRanges数组中获取对应的下标的偏移量，就是对应分区的偏移量
          val offsetRange: OffsetRange = offsetRanges(TaskContext.getPartitionId())
          //定义一个ArrayList 并指定长度 更高效 用于批量写入put
          val puts = new util.ArrayList[Put](100)
          val htable: Table = connection.getTable(TableName.valueOf("t_orders"))
          iter.foreach(order => {
            val oid: String = order.oid
            val totalMoney: Double = order.totalMoney
            //rowkey
            val put = new Put(Bytes.toBytes(oid))
            //data列族 和 offset列族
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("total_money"), Bytes.toBytes(totalMoney))
            //分区中的最后一条数据 将偏移量取出来 保存到hbase的列族
            if (!iter.hasNext) {
              put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes("appid_groupid"), Bytes.toBytes(appName + "_" + groupId))
              put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes("topic_partition"), Bytes.toBytes(offsetRange.topic + "_" + offsetRange.partition))
              //put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes("partition"), Bytes.toBytes(groupId))
              put.addColumn(Bytes.toBytes("offset"), Bytes.toBytes("offset"), Bytes.toBytes(offsetRange.untilOffset))
            }
            //缓存起来
            puts.add(put)
            //满足条件写入hbase
            if(puts.size() == 100){
              //写入100到hbase
              htable.put(puts)
              //清空puts
              puts.clear()
            }
          })
          //写入到hbase 最后不满100条的写入
          htable.put(puts)
        })

      }
    })
    ssc.start()
    ssc.awaitTermination()*/
  }
}

package com.yunkeji.spark.streaming.util

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import scala.collection.mutable

object OffsetUtils {
  def queryHistoryOffsetFromHbase(appName: String, groupId: String): scala.collection.Map[TopicPartition, Long] = {
    val offsetMap = new mutable.HashMap[TopicPartition, Long]
    val connection: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
    val sql = "select \"topic_partition\", max(\"offset\") from \"t_orders\" where \"appid_groupid\" = ? group by \"topic_partition\""
    val ps: PreparedStatement = connection.prepareStatement(sql)
    ps.setString(1, appName + "_" + groupId)

    val rs: ResultSet = ps.executeQuery()

    while (rs.next()) {
      val topicAndPartition: String = rs.getString(1)
      val fields: Array[String] = topicAndPartition.split("_")
      val topic: String = fields(0)
      val partition: String = fields(1)
      val offset: Long = rs.getLong(2)

      val topicPartition = new TopicPartition(topic, partition.toInt)
      offsetMap.put(topicPartition,offset)
    }

    offsetMap.toMap
  }


  def queryHistoryOffsetFromRedis(appId: String, groupId: String): scala.collection.Map[TopicPartition, Long] = {
    val offsetMap = new mutable.HashMap[TopicPartition, Long]
    val jedis: Jedis = JedisConnectionPool.getConnection()
    jedis.select(15)
    val map: util.Map[String, String] = jedis.hgetAll(appId + "_" + groupId)
    //导入隐式转换
    import scala.collection.JavaConverters._

    for (tp <- map.asScala) {
      val topic_partition: String = tp._1
      val arr: Array[String] = topic_partition.split("_")
      val topic: String = arr(0)
      val partition: String = arr(1)
      val offset: String = tp._2
      val topicPartition = new TopicPartition(topic, partition.toInt)
      //offsetMap.put(topicPartition,offset.toLong)
      offsetMap(topicPartition) = offset.toLong
    }
    offsetMap.toMap
  }

  //从mysql 查询历史偏移量
  def queryHistoryOffsetFromMySql(appId: String, groupId: String): collection.Map[TopicPartition, Long] = {
    val offsetMap = new mutable.HashMap[TopicPartition, Long]
    var connection: Connection = null
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      connection = DruidConnectionPool.getConnection
      statement = connection.prepareStatement("select topic_partition , offset from t_offset where app_id = ?")
      resultSet = statement.executeQuery()
      while (resultSet.next()) {
        val topic_partition: String = resultSet.getString(1)
        val offset: Long = resultSet.getLong(2)
        val fields: Array[String] = topic_partition.split("_")
        val topic: String = fields(0)
        val partition: String = fields(1)
        val topicPartition = new TopicPartition(topic, partition.toInt)
        offsetMap.put(topicPartition, offset)
      }

    } catch {
      case e: Exception =>
        //抱出去 程序会停止  自己捕获的话不会
        throw new RuntimeException("查询历史偏移量失败", e)
    } finally {
      if (resultSet != null) resultSet.close()
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
    offsetMap.toMap
  }
}

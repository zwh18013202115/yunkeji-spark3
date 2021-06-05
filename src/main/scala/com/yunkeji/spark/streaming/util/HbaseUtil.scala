package com.yunkeji.spark.streaming.util

/*
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
*/

/**
 * Hbase的工具类 用来创建habase的连接
 */
object HbaseUtil {
  /**
   * zkQuorum zookeeper的的地址 多个用逗号隔开
   * port zookeeper的端口号 2181
   *
   * @param zkQuorum
   * @param port
   * @return
   */
/*  @volatile var connection: Connection = null

  def getConnection(zkQuorum: String, port: Int): Connection = {
    if (connection == null) {
      synchronized {
        if (connection == null) {
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", zkQuorum)
          conf.set("hbase.zookeeper.property.clientPort", port.toString)
          connection = ConnectionFactory.createConnection(conf)
        }
      }
    }
    connection
  }*/


}

package com.yunkeji.spark.streaming

/*import com.yunkeji.spark.streaming.util.HbaseUtil
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection}*/

object HbaseTest {
  def main(args: Array[String]): Unit = {
/*
    val connection: Connection = HbaseUtil.getConnection("hadoop101,hadoop102,hadoop103", 2181)
    //val connection: Connection = HbaseUtil.getConnection("hadoop101", 2181)
    import org.apache.hadoop.hbase.client.HBaseAdmin
    //在HBase中管理、访问表需要先创建HBaseAdmin对象
    //Connection connection = ConnectionFactory.createConnection(conf);
    //HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
    //val admin = new HBaseAdmin(conf)

    //1.创建表管理类
    val admin: Admin = connection.getAdmin

    //2.创建表描述类
    val tableName: TableName = TableName.valueOf("test2")//表名称
    val desc = new HTableDescriptor(tableName)

    //3.创建列族的描述类

    val family1 = new HColumnDescriptor("info1")
    //4.添加到表中
    desc.addFamily(family1)
    val family2 = new HColumnDescriptor("info2")
    desc.addFamily(family2)

    //5.创建表
    admin.createTable(desc)

    println("ddd")
*/


  }
}

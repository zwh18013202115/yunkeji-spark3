package com.yunkeji.spark.streaming.exactlyonce

import com.yunkeji.spark.streaming.util.DruidConnectionPool

import java.sql.{Connection, PreparedStatement}

object MysqlSaveOffset {
  def main(args: Array[String]): Unit = {

    var connection: Connection = null
    var pstm: PreparedStatement = null
    try {
      connection = DruidConnectionPool.getConnection
      //关闭自动提交事务
      connection.setAutoCommit(false)
      //先用？？占位防止注入
      var sql = "INSERT INTO test(words,count) values(?,?)"
      pstm = connection.prepareStatement(sql)
      pstm.setString(1, "flink")
      pstm.setLong(2, 2000L)

      pstm.setString(1, "kafka")
      pstm.setLong(2, 3000L)
      // pstm.addBatch()
      // pstm.execute()
      pstm.executeUpdate()
      //提交事务
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()

        connection.rollback()
    } finally {
      if (pstm != null) {
        pstm.close()
      }
      if (connection != null) connection.close()
    }


  }
}

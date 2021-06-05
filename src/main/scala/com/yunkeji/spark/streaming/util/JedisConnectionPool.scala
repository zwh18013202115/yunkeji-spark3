package com.yunkeji.spark.streaming.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  val config = new JedisPoolConfig
  config.setMaxIdle(5)
  config.setMaxTotal(5)
  config.setTestOnBorrow(true)
  //private val pool = new JedisPool(config, "hadoop101", 6379, 5000, "123456")
  private val pool = new JedisPool(config, "hadoop101", 6379, 5000)

  def getConnection() = {
    pool.getResource
  }
}

package com.yunkeji.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

/**
 * 现在2.0的还在用 3.0有新的
 */
class MyAVGDefindFunction extends UserDefinedAggregateFunction {
  //参数的输入类型 + 自己定义的 可用 Seq util.List Array 来包装
  override def inputSchema: StructType = StructType(Seq(StructField("input", DoubleType)))

  //中间要缓存的数据类型
  override def bufferSchema: StructType = {
    StructType(Seq(
      StructField("total", DoubleType), //总的累加
      StructField("amount", IntegerType) //数量  最后要求平均值
    ))
  }

  //返回的的数据类型 DataType
  override def dataType: DataType = DoubleType

  //输入的类型和还回的是否一样
  override def deterministic: Boolean = true

  //初始化的初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0 //薪水大的初始值
    buffer(1) = 0 //员工的数量初始值
  }

  //局部聚合的方法，每一个组（在每个分区内），每来一条数据调用该方法
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //buffer.getDouble(0) 取值  相加 再赋值
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    //人数 +1
    buffer(1) = buffer.getInt(1) + 1
  }

  //全局聚合调用的函数
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)

    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //最后返回结果
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)/buffer.getInt(1)
  }
}

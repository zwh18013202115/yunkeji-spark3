package com.yunkeji.spark.rdd

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

object ReuceByKeyDdemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.parallelize(
      List(
        "spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka",
        "hadoop", "flink", "hive", "flink"
      ), 4)

    /**
     * reduceByKey相当于被调用两次
     * 分区内  分区间
     *
     * 也就是局部聚合 全局聚合
     */
    val reduceByKeyRDD: RDD[(String, Int)] = rdd.map((_, 1)).reduceByKey(_ + _)
    reduceByKeyRDD.collect().foreach(println(_))

    //使用 groupByKey 来实现 reduceByKey的功能   但是：数据量很大的时候reduceByKey效率高一点，reduceByKey会先局部聚合，再全局聚合
    //mapValues: 对value操作 最后 把key并接上
    //val groupRDD: RDD[(String, Iterable[Int])] = rdd.map((_, 1)).groupByKey()
    //val res: RDD[(String, Int)] = groupRDD.mapValues(_.sum)
    //Thread.sleep(1000000)

    //自定义实现reduceByKey功能
    val wordsOne: RDD[(String, Int)] = rdd.map((_, 1))

    /**
     * combineByKey 所需参数
     * createCombiner: V => C,
     * mergeValue: (C, V) => C,
     * mergeCombiners: (C, C) => C,
     * partitioner: Partitioner,
     * mapSideCombine: Boolean = true,
     * serializer: Serializer = null): RDD[(K, C)]
     *
     * 也就是说：reduceByKey 局部聚合和全局聚合的处理逻辑必须一样
     * 而 combineByKey可以自定义局部，全局的聚合 可以不一样
     */
    /*  //取出key的value 不处理
      val createCombiner = (x: Int) => x
      //局部聚合
      val mergeValue = (m: Int, n: Int) => m + n
      //全局聚合
      val mergeCombiners = (a: Int, b: Int) => a + b
      val result: RDD[(String, Int)] = wordsOne.combineByKey(createCombiner, mergeValue, mergeCombiners)*/


    /**
     * 使用 new ShuffledRDD实现类似 reduceByKey 的功能
     * class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
     * @transient var prev: RDD[_ <: Product2[K, V]],
     *            part: Partitioner)
     */

    val resultShuffledRDD: ShuffledRDD[String, Int, Int] = new ShuffledRDD[String, Int, Int](
      wordsOne,
      new HashPartitioner(wordsOne.partitions.length))
    //设置 map 聚合
    resultShuffledRDD.setMapSideCombine(true)
    //取出key的value 不处理
    val createCombiner = (x: Int) => x
    //局部聚合
    val mergeValue = (m: Int, n: Int) => m + n
    //全局聚合
    val mergeCombiners = (a: Int, b: Int) => a + b
    resultShuffledRDD.setAggregator(new Aggregator[String,Int,Int](createCombiner,mergeValue,mergeCombiners))
    sc.stop()
  }
}

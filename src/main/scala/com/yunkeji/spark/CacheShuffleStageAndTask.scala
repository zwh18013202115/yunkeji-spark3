package com.yunkeji.spark

import com.yunkeji.spark.util.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CacheShuffleStageAndTask {
  /**
   * RDD:三种:(HadoopRDD读取文件) MapPartitionsRDD SuffledRDD  ResultRDD
   *
   * 概念：  stage-->1.SuffleMapStage,2.resultStage对应resultTask就是产生结果
   * 概念：  task-->1.SuffleMapTask:应用分区器-shufflewriter写磁盘(下游 shuffleread来读上游的数据)
   * 2.resultTask就是产生结果 写道外部系统 ***
   *
   * 分区：hadoopRDD 切片数 或者小于1.1*128M的文件个数
   * task的个数 = 分区数 * stage阶段个数
   *
   * 一个action算子触发一个job-->对应一个或者多个stage,也是taskset-->多个task
   *
   * 特殊情况：没有shuffle  就一个stage  就一个resultStage对应resultTask  就像sqoop
   *
   * Cache 既不是transformation也不是action 返回rdd本身：存储级别 默认 MemoryOnly  要多次用到才有用  Cache可以缓存一部分 剩下的从数据圆度
   *
   * checkpoint一般是机器学习或者很复杂的计算  一般结合persist使用
   *
   * shuffle是 一种特殊的 缓存 会写的到各个executor的所在机器
   * /tmp/spark-xxx 的目录里 会有两个文件 shuffle-index 和shuffle-data
   * 当文件被删除 时 会出现拉取失败的异常 然后再触发时重新计算 在写到磁盘
   *
   *
   * sortSuffle--->有三种情况
   * 1.groupby (因为map端没有聚合) 且分区小于200--->BypassMergeSortWriter
   * 2.使用kyo系列化，且底层没有调用聚合器new Aggregator，分区小于16777215 如：repartitionAndSortWithinPartitions(myPartioner)-UnsafeSuffleWriter
   * 3.其他的情况 sortShuffleWriter
   *
   *
   * CoarseGrainedExecutorBackend :worker启动里面的 main方法-->调用run方法-->
   * onstart方法（向driver发送注册executor--driver返回注册成功消息-->new Executor()里面--threadPoll线程池）
   * -->向driver返回消息 线程池准备就绪-->driver收到消息后 发Task（executor.launchTask(..task的描述信息 new TaskRunner(context,taskDescription))）
   * -->executor接受到发送过来的task 后通过反系列化 task=ser.deserialize[Task[Any]](...)
   *
   * new ShuffledBlockFetcherIterator 可以从本地 或者 网络读取 上游的数据 -->返回一个迭代器
   */

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkUtil.getSparkSession(this.getClass.getName)
    val sc: SparkContext = spark.sparkContext
    //Grafana
    val rdd = sc.parallelize(args(0))
    rdd.saveAsTextFile("")
  }
}

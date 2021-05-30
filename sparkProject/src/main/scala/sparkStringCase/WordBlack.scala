/**
  * @Auther icebear
  * @Date 5/30/21
  */

package sparkStringCase

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 黑名单过滤
  */
object WordBlack {
  def main(args: Array[String]): Unit = {
    // 参数配置
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    // 数据输入
    val wordBlackList = ssc.sparkContext.parallelize(List("?", "!", "*"))
      .map(param => (param, true))
    val blackList = wordBlackList.collect()
    // 设置广播变量
    val blackListBroadcast = ssc.sparkContext.broadcast(blackList)
    // 接收socket端发送的数据
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 8022)
    // 数据处理，join操作
    val wordOneDStream = dstream.flatMap(_.split(","))
      .map((_, 1))
    val wordCountDStream = wordOneDStream.transform(rdd => {
      // 将广播变量转换成RDD
      val filterRDD: RDD[(String, Boolean)] = rdd.sparkContext.parallelize(blackListBroadcast.value)
      val resultRDD: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(filterRDD)
      // 过滤掉黑名单值
      resultRDD.filter(tuple => {
        tuple._2._2.isEmpty
      }).map(_._1)
    }).map((_, 1)).reduceByKey(_ + _)
    // 数据输出
    wordCountDStream.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

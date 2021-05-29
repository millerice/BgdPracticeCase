/**
  * @Auther icebear
  * @Date 5/29/21
  */

package sparkStringCase

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 单词计数
  */
object UpdateStateBykeyWordCount {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    // checkpoint路径
    ssc.checkpoint("hdfs://node01:8020/streamingCheckpoint")
    // 数据输入
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 8020)
    // 数据处理
    val wordCountDStream = dstream.flatMap(_.split(","))
      .map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val currentCount = values.sum
        val lastCount = state.getOrElse(0)
        Some(currentCount + lastCount)
      })
    // 数据输出
    wordCountDStream.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

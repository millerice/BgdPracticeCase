/**
  * @Auther icebear
  * @Date 5/30/21
  */

package sparkStringCase

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// driver HA
object DriverHAWordCount {
  def main(args: Array[String]): Unit = {
    // checkpoint 路径
    val checkpointDirectory: String = "hdfs://node01:8020/streamingCheckpoint"
    // 配置信息
    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(sc, Seconds(5))
      ssc.checkpoint(checkpointDirectory)
      // 获取数据
      val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 8022)
      // 数据处理
      val wordCountDStream = dstream.flatMap(_.split(","))
        .map((_, 1))
        .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
          val currentCount = values.sum
          val lastCount = state.getOrElse(0)
          Some(currentCount + lastCount)
        })
      wordCountDStream.print()
      ssc.start()
      ssc.awaitTermination()
      ssc.stop()
      ssc
    }
    // 数据存在则直接获取，不存在则创建
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    // 开启streaming
    ssc.start()
    // 等待
    ssc.awaitTermination()
    // 关闭
    ssc.stop()
  }
}

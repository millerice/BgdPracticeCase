/**
  * @Auther icebear
  * @Date 5/30/21
  */

package sparkStringCase

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 实现每隔4s统计最近6s的单词数量
  */
object WindowOperatorTest {
  def main(args: Array[String]): Unit = {
    // 参数配置
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    // 数据输入
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 8022)
    // 数据处理
      // 生产中用到的算子
      // reduceFunc: (V, V) => V,
      // windowDuration: Duration,6 窗口的大小
      // slideDuration: Duration,4  滑动的大小
      // numPartitions: Int  指定分区数
    val resultWordCountDStream = dstream.flatMap(_.split(","))
        .map((_, 1))
        .reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(6), Seconds(4))
    // 数据输出
    resultWordCountDStream.print()
    // 开启 等待 关闭
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

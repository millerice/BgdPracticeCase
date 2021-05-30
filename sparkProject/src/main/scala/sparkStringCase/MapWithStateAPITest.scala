/**
  * @Auther icebear
  * @Date 5/30/21
  */

package sparkStringCase

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StateSpec, StreamingContext}

object MapWithStateAPITest {
  def main(args: Array[String]): Unit = {
    // 配置属性
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    // checkpoint路径
    ssc.checkpoint("hdfs://node01:8020/streamingCheckpoint")
    // 从socket端接收数据
    val lines = ssc.socketTextStream("node01", 8022, StorageLevel.MEMORY_AND_DISK_SER)
    // 数据切分
    val words = lines.flatMap(_.split(","))
    val wordsDStream = words.map(x => (x, 1))
    // 数据转换成rdd
    val initialRDD = sc.parallelize(List(("dummy", 100L), ("source", 32L)))
    // 数据上下文处理
//    val stateSpec = StateSpec.function((currentBatchTime: Time, key: String,
//                                        value: Option[Int], currentState: State[Long]) => {
//      val sum = value.getOrElse(0).toLong + currentState.getOption.getOrElse(0L)
//      val output = (key, sum)
//      if (!currentState.isTimingOut()){
//        currentState.update(sum)
//      }
//      Some(output)
//    }).initialState(initialRDD).numPartitions(2).timeout(Seconds(30))
    // 使用mapWithState处理
//    val result = wordsDStream.mapWithState(stateSpec)
    // 结果打印
//    result.print()
//    result.stateSnapshots().print()
    // 启动streaming处理流
//    ssc.start()
    // 停止
//    ssc.stop(false)
    // 等待程序结束
//    ssc.awaitTermination()
  }
}

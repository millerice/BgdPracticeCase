/**
  * @Auther icebear
  * @Date 5/30/21
  */

package sparkStringCase

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sparkStreaming 整合 sparkSQL
  */
object NetworkWordCountForeachRDDDataFrame {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val conf = new SparkConf().setAppName("NetworkWordCountForeachRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    // 接收数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 8022)
    // 数据处理
    val words = lines.flatMap(_.split(" "))
    // 将rdd数据转换成Dataset数据
    words.foreachRDD{rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      // 数据转换
      val wordsDataFrame = rdd.toDF("word")
      // 构建临时表
      wordsDataFrame.createOrReplaceTempView("words")
      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    }
    // 启动Streaming处理流
    ssc.start()
    ssc.stop(false)
    // 将RDD转化为Dataset
    words.foreachRDD{ (rdd, time) =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val wordsDataFrame = rdd.toDF("words")
      val wordCountsDataFrame = wordsDataFrame.groupBy("word").count()
      val resultDFWithTs = wordCountsDataFrame.rdd.map(row => (row(0), row(1), time.milliseconds))
        .toDF("word", "count", "ts")
      // 将数据写入hdfs
      resultDFWithTs.write.mode(SaveMode.Append).parquet("hdfs://node01:8020/streamingCheckpoint/parquet")
    }
    // 等待Streaming程序终止
    ssc.awaitTermination()
  }
}

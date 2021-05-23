/**
  * @Auther icebear
  * @Date 5/23/21
  */

package mypartitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestPartitionerMain {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("TestPartitionerMain").setMaster("local[2]")
    // 2.构建SparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    // 3.构建数据源
    val data: RDD[String] = sc.parallelize(List("hadoop", "hdfs", "hive", "spark", "flume", "kafka", "flink"))
    // 4.获取每一个元素的长度，封装成一个元组
    val wordLengthRDD: RDD[(String, Int)] = data.map(x =>(x, x.length))
    // 5.对应上面的rdd数据进行自定义分区
    val result: RDD[(String, Int)] = wordLengthRDD.partitionBy(new MyPartitioner(3))
    // 6.保存结果数据到文件
    result.saveAsTextFile("./data")
    sc.stop()
  }
}

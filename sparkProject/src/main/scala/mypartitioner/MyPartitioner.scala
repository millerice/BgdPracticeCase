/**
  * @Auther icebear
  * @Date 5/23/21
  */
package mypartitioner

import org.apache.spark.Partitioner

// 自定义分区
class MyPartitioner(num:Int) extends Partitioner {
  // 指定RDD总分区数
  override def numPartitions: Int = {
    num
  }

  // 消息按照key的某种规则进入到指定的分区号中
  override def getPartition(key: Any): Int = {
    // 这里的key就是单词
    val length: Int = key.toString.length
    length match {
      case 4 => 0
      case 5 => 1
      case 6 => 2
      case _ => 0
    }
  }
}

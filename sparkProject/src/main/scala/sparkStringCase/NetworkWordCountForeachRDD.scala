/**
  * @Auther icebear
  * @Date 5/30/21
  */

package sparkStringCase

import java.sql.DriverManager

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scalikejdbc.ConnectionPool

// sparkStreaming 实时消费TCP server 发过来的数据，并将其保存到mysql中
object NetworkWordCountForeachRDD {
  def main(args: Array[String]): Unit = {
    // 配置信息
    val sparkConf = new SparkConf().setAppName("NetworkWordCountForeachRDD").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    // 接收数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 8022)
    // 处理数据
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
//    方法一
    // 将数据保存到mysql
//    wordCounts.foreachRDD{(rdd, time) =>
//      Class.forName("com.mysql.jdbc.Driver")
//      val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/testDB", "root", "123456")
//      val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
//      rdd.foreach { record =>
//        statement.setLong(1, time.milliseconds)
//        statement.setString(2, record._1)
//        statement.setInt(3, record._2)
//        statement.execute()
//      }
//      // 关闭资源
//      statement.close()
//      conn.close()
//    }
//    // 启动Streaming处理流
//    ssc.start()
//    // 关闭流
//    ssc.stop()

//    方法二
    wordCounts.foreachRDD{ (rdd, time) =>
      rdd.foreachPartition{ partitionRecords =>
        val conn = ConnectionPool.getConnection
        conn.setAutoCommit(false)
        val statement = conn.prepareStatement(s"insert into wordcount(ts, word, count) values (?, ?, ?)")
        partitionRecords.zipWithIndex.foreach{ case ((word, count), index) =>
            statement.setLong(1, time.milliseconds)
            statement.setString(2, word)
            statement.setInt(3, count)
            statement.addBatch()
            if (index != 0 && index % 500 == 0){
              statement.executeBatch()
              conn.commit()
            }
        }
        statement.executeBatch()
        statement.close()
        conn.commit()
        conn.setAutoCommit(true)
        ConnectionPool.returnConnection(conn)
      }
    }

    // 等待程序终止
    ssc.awaitTermination()
  }
}

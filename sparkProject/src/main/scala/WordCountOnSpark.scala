import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther icebear
  * @Date 5/19/21
  */

// 集群运行spark单词统计程序
object WordCountOnSpark {
  def main(args: Array[String]): Unit = {
    // 1.构建sparkConf对象 设置application名称
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountSpark")
    // 2.构建sparkContext对象
    val sc = new SparkContext(sparkConf)
    // 3.设置日志输出级别
    sc.setLogLevel("warn")
    // 4.读取数据文件
    val data: RDD[String] = sc.textFile(args(0))
    // 5.切分每一行
    val words:RDD[String] = data.flatMap(x => x.split(" "))
    // 6.每个单词记为1
    val wordAndOne:RDD[(String, Int)] = words.map(x => (x, 1))
    // 7.相同的单词累加
    val result:RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x + y)
    // 8.计算结果保存到hdfs上
    result.saveAsTextFile(args(1))
    // 9.关闭sc
    sc.stop()
  }
}

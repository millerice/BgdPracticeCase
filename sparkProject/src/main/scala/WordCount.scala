import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther icebear
  * @Date 5/19/21
  */

// spark实现单词统计
object WordCount {
  def main(args: Array[String]): Unit = {
    // 1.构建sparkConf对象，设置application名称和master地址
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    // 2.构建sparkConf对象，它是spark程序的入口
    val sc = new SparkContext(sparkConf)
    // 3.设置日志输出级别
    sc.setLogLevel("warn")
    // 4.读取数据文件
    val data: RDD[String] = sc.textFile("/Users/wangkai/Desktop/bigData/testData/words.txt")
    // 5.切分每一行，获取所有单词
    val words: RDD[String] = data.flatMap(x=>x.split(" "))
    // 每个单词记为1
    val wordAndOne: RDD[(String, Int)] = words.map(x => (x, 1))
    // 6.相同单词出现的累加1
    val result: RDD[(String, Int)] = wordAndOne.reduceByKey((x, y) => x + y)
    // 7.按照单词出现的次数降序排列 第二个参数默认是true表示升序 设置为false表示降序
    val sortedRDD: RDD[(String, Int)] = result.sortBy(x => x._2, false)
    // 8.收集数据打印
    val finalResult: Array[(String, Int)] = sortedRDD.collect()
    finalResult.foreach(println)
    // 9.关闭sc
    sc.stop()
  }
}

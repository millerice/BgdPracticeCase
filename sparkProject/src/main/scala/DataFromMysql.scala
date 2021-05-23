import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Auther icebear
  * @Date 5/23/21
  */

// 利用sparksql加载mysql中的数据
object DataFromMysql {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DataFromMysql").setMaster("local[2]")
    // 2.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 3.读取mysql的数据
    val url = "jdbc:mysql://node03:3306/userdb"
    val tableName = "emp"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, properties)
    // 4.打印schema信息
    mysqlDF.printSchema()
    // 5.展示数据
    mysqlDF.show()
    // 6.把dataFrame注册成表
    mysqlDF.createTempView("user")
    spark.sql("select * from user where age > 18").show()
    spark.stop()
  }
}

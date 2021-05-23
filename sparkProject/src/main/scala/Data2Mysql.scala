import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Auther icebear
  * @Date 5/23/21
  */

// 通过sparksql把结果数据写入mysql表中
object Data2Mysql {
  def main(args: Array[String]): Unit = {
    // 1.创建SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Data2Mysql")
//      .master("local[2]") // 本地运行
      .getOrCreate()
    // 2.读取mysql表中的数据
    // 3.定义url连接
    val url = "jdbc:mysql://node03:3306/userdb"
    // 4.定义表名
    val table = "emp"
    // 5.定义属性
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    val mysqlDF: DataFrame = spark.read.jdbc(url, table, properties)
    // 6.把dataFrame定义成一张表
    mysqlDF.createTempView("user")
    // 7.通过sparkSession调用sql方法
    val result: DataFrame = spark.sql("select * from user where age > 18")
    // 8.将数据写入mysql表中
      //mode:指定数据的插入模式
      //overwrite: 表示覆盖，如果表不存在，事先帮我们创建
      //append   :表示追加， 如果表不存在，事先帮我们创建
      //ignore   :表示忽略，如果表事先存在，就不进行任何操作
      //error    :如果表事先存在就报错（默认选项
    // 本地运行
//    result.write.mode("append").jdbc(url, "sparkTb", properties)
    // 服务器端运行（jar包）
    result.write.mode(args(0)).jdbc(url, args(1), properties)
    // 9.关闭
    spark.stop()
  }

}

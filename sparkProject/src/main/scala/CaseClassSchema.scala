/**
  * @Auther icebear
  * @Date 5/23/21
  */

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Person(id: String, name: String, age: Int)

// 利用反射机制实现把rdd转成dataFrame
object CaseClassSchema {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("CaseClassSchema").master("local[2]").getOrCreate()
    // 2.获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    // 3.读取文件数据
    val data: RDD[Array[String]] = sc.textFile("/Users/wangkai/Desktop/bigData/testData/person.txt").map(x=>x.split(" "))
    // 4.定义一个样例类(外部定义)
    // 5.将rdd与样例类进行关联
    val personRDD: RDD[Person] = data.map(x=>Person(x(0), x(1), x(2).toInt))
    // 6.将rdd转换成dataFrame, 需要手动导入隐式转换
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF
    // 8.对dataFrame进行相应语法操作
//    personDF.printSchema()
//    personDF.show()
    // DSL风格语法®
    //val top3: Array[Row] = personDF.head(3)
    // val first: Row = personDF.first()
    // println("first:" + first)
    //top3.foreach(println())
//    personDF.select("name").show()
//    personDF.select("name", "age").show()
//    personDF.select($"name", $"age", $"age" + 1).show()
//    personDF.groupBy("age").count().show()
//    personDF.filter($"age" > 30).show()
//    personDF.foreach(row => println(row))
    // SQL风格语法
    personDF.createTempView("person")
    // 使用SparkSession调用sql方法统计查询
//    spark.sql("select * from person").show()
//    spark.sql("select name from person").show()
//    spark.sql("select name, age from person").show()
    spark.sql("select * from person order by age desc").show()

    spark.stop()
  }
}

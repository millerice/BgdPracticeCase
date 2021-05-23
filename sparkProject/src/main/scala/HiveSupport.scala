import org.apache.spark.sql.SparkSession

/**
  * @Auther icebear
  * @Date 5/23/21
  */

// 利用sparksql操作hivesql
object HiveSupport {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession对象
    val spark:SparkSession = SparkSession.builder()
      .appName("HiveSupport")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    // 2.直接使用sparkSession去操作hivesql语句
    // 3.创建一张hive表
    spark.sql("create table people(id string, name string, age int) row format delimited fields terminated by ','")
    // 4.加载数据到hive表中
    spark.sql("load data local inpath '/Users/wangkai/Desktop/bigData/testData/person.txt' into table people")
    // 5.查询
    spark.sql("select * from people").show()
    spark.stop()
  }

}

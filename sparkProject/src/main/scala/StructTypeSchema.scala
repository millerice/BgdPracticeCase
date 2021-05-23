import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField}

/**
  * @Auther icebear
  * @Date 5/23/21
  */

// 通过动态指定dataFrame对应的schema信息将rdd转换成dataFrame
object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    // 1.构建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("StructTypeSchema").master("local[2]").getOrCreate()
    // 2.获取sparkContext对象
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    // 3.读取文件数据
    val data: RDD[Array[String]] = sc.textFile("/Users/wangkai/Desktop/bigData/testData/person.txt").map(x => x.split(" "))
    // 4.将rdd与Row对象进行关联
    val rowRDD: RDD[Row] = data.map(x=>Row(x(0), x(1), x(2).toInt))
    // 5.指定dataFrame的schema信息，这里指定的字段个数必须要跟Row对象保持一致
    val schema = StructType(
      StructField("id", StringType)::
      StructField("name", StringType)::
      StructField("age", IntegerType)::Nil
    )
    // 6.查询
    val dataFrame: DataFrame = spark.createDataFrame(rowRDD, schema)
    dataFrame.printSchema()
    dataFrame.show()
    dataFrame.createTempView("user")
    spark.sql("select * from user").show()

    spark.stop()
  }

}

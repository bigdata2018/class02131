package SQL_two

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * description: N8 
 * date: 2020/6/20 8:46 
 * author: nogc
 * version: 1.0 
 */
object N8 {
  def main(args: Array[String]): Unit = {
readWrite()
  }

  def readWrite() = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("cc")
      .getOrCreate()

    import spark.implicits._
//    val csvRDD: DataFrame = spark.read
//      .option("header", true)
//      .csv("input/BeijingPM20100101_20151231.csv")
//
//    csvRDD.write
//      .partitionBy("year", "month")
//      .save("output/mx1")

    spark.read.parquet("output/mx1")
      .printSchema()

  }
}

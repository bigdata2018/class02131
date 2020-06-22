package SQL_two

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * description: N9_Json
 * date: 2020/6/20 9:02 
 * author: nogc
 * version: 1.0 
 */
object N9_Json {
  def main(args: Array[String]): Unit = {
readWrite()
  }

  def readWrite() = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("cc")
      .getOrCreate()

    import spark.implicits._
/*        val csvRDD: DataFrame = spark.read
          .option("header", true)
          .csv("input/BeijingPM20100101_20151231.csv")

    csvRDD.write
      .json("output/mx2.json")*/

    spark.read.json("output/mx2.json").show()
  }

}
package SQL_two

import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * description: N6_read 
 * date: 2020/6/19 21:35 
 * author: nogc
 * version: 1.0 
 */
object N6_read {
  def main(args: Array[String]): Unit = {
    dataframe()
  }

  def dataframe() = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("mx")
      .getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.read
      .option("header", value = true)
      .csv("input/BeijingPM20100101_20151231.csv")

    //df.printSchema()
//    df.select('year,'month,'PM_Dongsi)
//      .where('PM_Dongsi =!= "NA")
//      .groupBy('year,'month)
//      .count()
//      .show()
/*
    df.createOrReplaceTempView("pm")
    val result: DataFrame = spark.sql("select year,month,count(PM_Dongsi) from pm  where PM_Dongsi != 'NA' group by year,month")
    result.show()*/
    df.write.json("output/cc.json")
    df.write.format("json").save("output/cc1.json")
    spark.stop()
  }
}

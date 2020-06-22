package SQL.day1

import org.apache.spark.sql.SparkSession

/**
 * description: HiveDemo 
 * date: 2020/6/12 22:25 
 * author: nogc
 * version: 1.0 
 */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql

    sql("select * from aa").show
  }

}

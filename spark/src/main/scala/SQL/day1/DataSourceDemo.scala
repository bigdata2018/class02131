package SQL.day1

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * description: DataSourceDemo 
 * date: 2020/6/12 23:13 
 * author: nogc
 * version: 1.0 
 */
object DataSourceDemo {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("input/user.json")
    val ds: Dataset[User] = df.as[User]
    ds.foreach(user => println(user.friends(0)))

  }
}
case class User1(name:String,age:Long,friends:Array[String])

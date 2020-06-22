package SQL.day1


import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * description: DataSource 
 * date: 2020/6/12 23:27 
 * author: nogc
 * version: 1.0 
 */
object DataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    import spark.implicits._

    val jsonDF: DataFrame = spark.read.json("input/user.json")
    jsonDF.write.mode(SaveMode.Overwrite).parquet("output/user.parquet")

    val parDF: DataFrame = spark.read.parquet("output/user.parquet")
   val userDS: Dataset[User] = parDF.as[User]

    userDS.foreach(user => println(user.age))
  }
}
case class User(var name:String, age: Long, friends: Array[String])

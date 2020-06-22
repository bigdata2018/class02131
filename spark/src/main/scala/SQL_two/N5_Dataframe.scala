package SQL_two

import org.apache.spark.sql.SparkSession

/**
 * description: N5_Dataframe 
 * date: 2020/6/19 18:32 
 * author: nogc
 * version: 1.0 
 */
object N5_Dataframe {
  def main(args: Array[String]): Unit = {
    println(dataframe())
  }

  def dataframe() = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("cc")
      .getOrCreate()
    import spark.implicits._
    val dataFrame = Seq(Person("zhangsan", 9), Person("lisi", 15)).toDF()
    dataFrame.where('age > 10)
      .select('name)
      .show()

  }
}

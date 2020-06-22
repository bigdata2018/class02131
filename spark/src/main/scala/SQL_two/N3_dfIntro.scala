package SQL_two

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * description: N3_dfIntro
 * date: 2020/6/19 17:14
 * author: nogc
 * version: 1.0
 */
object N3_dfIntro {
  def main(args: Array[String]): Unit = {
println(dsIntro())
  }
  case class Person(name:String,age:Int)
  def dsIntro() = {
    val spark = new SparkSession.Builder()
      .master("local[*]")
      .appName("cc")
      .getOrCreate()
    import spark.implicits._
    val sourceRDD: RDD[Persion] = spark.sparkContext.parallelize(Seq(Persion("cc", 17), Persion("ff", 19)))
    val df: Dataset[Persion] = sourceRDD.toDS()
    df.createOrReplaceTempView("cc")
    val frame: DataFrame = spark.sql("select name from cc where age >10 and age < 20")
    frame.show()
  }
}

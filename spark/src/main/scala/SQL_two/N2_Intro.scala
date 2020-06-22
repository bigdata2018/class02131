package SQL_two



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


/**
 * description: N2_Intro 
 * date: 2020/6/19 16:04 
 * author: nogc
 * version: 1.0 
 */
object N2_Intro {
/*  @Test
  def rddIntro() = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("cc")
    val sc = new SparkContext(conf)
    sc.textFile("input/word.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println(_))
    sc.stop()
  }*/
def main(args: Array[String]): Unit = {
  println(dsIntro)
}
  def dsIntro() = {
    val spark = new SparkSession.Builder()
      .master("local[*]")
      .appName("cc")
      .getOrCreate()

    import spark.implicits._
    val sourceRDD: RDD[Persion] = spark.sparkContext.parallelize(Seq(Persion("cc", 10), Persion("DD", 11)))
    val personDS: Dataset[Persion] = sourceRDD.toDS()

    val resultDS: Dataset[String] = personDS.where('age > 10)
      .where('age < 18)
      .select('name)
      .as[String]
    resultDS.show()
  }
}
case  class Persion(name:String,age:Int)
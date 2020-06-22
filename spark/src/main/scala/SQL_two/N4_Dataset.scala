package SQL_two

import SQL_two.N4_Dataset.dataset
import org.apache
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * description: N4_Dataset 
 * date: 2020/6/19 17:40 
 * author: nogc
 * version: 1.0 
 */
object N4_Dataset {
  def main(args: Array[String]): Unit = {
    dataset()
  }

  def dataset() = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("cc")
      .getOrCreate()

    import spark.implicits._
    val dataset: Dataset[Person] = spark.createDataset(Seq(Person("zhangsan", 9), Person("lisi", 15)))
    //    sourceRDD.filter('age >10).show()
    /*val sourceRDD: RDD[Person] = spark.sparkContext.parallelize(Seq(Person("zhangsan", 9), Person("lisi", 15)))
    val dataset: Dataset[Person] = sourceRDD.toDS()*/
    /* dataset.filter('age > 10).show()
     dataset.filter(item => item.age >10).show()*/
    // dataset.explain(true)
    val executionRDD: RDD[InternalRow] = dataset.queryExecution.toRdd
    val rdd1: RDD[Person] = dataset.rdd
    println(executionRDD.toDebugString)
    println()
    println()
    println()
    println(rdd1.toDebugString)

  }
}

case class Person(name: String, age: Int)
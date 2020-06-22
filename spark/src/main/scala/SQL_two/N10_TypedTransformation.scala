package SQL_two

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * description: N10_TypedTransformation 
 * date: 2020/6/20 12:56 
 * author: nogc
 * version: 1.0 
 */
object N10_TypedTransformation {
  def main(args: Array[String]): Unit = {
    trans()
  }

  val spark = SparkSession.builder().master("local[*]").appName("cc").getOrCreate()

  import spark.implicits._

  def trans() = {
    val ds1: Dataset[String] = Seq("helllo cc", "hello aa").toDS()
    ds1.flatMap(item => item.split(" ")).show()

    val ds2 = Seq(Person("zh", 15), Person("li", 20)).toDS()
    ds2.map(person => Person(person.name, person.age * 2)).show(
    )
    ds2.mapPartitions(
      iter=>{
        iter.map(person =>Person(person.name,person.age*2))
        iter
      }
    ).show()

  }
}

package SQL_two

import org.apache
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * description: N7_toDS_toDF
 * date: 2020/6/19 22:15
 * author: nogc
 * version: 1.0
 */
object N7_toDS_toDF {
  def main(args: Array[String]): Unit = {
    dfds()
  }

  def dfds() = {
    val spark: SparkSession = new SparkSession.Builder()
      .master("local[*]")
      .appName("cc")
      .getOrCreate()


    import spark.implicits._


    val persons = Seq(Person("cc", 13), Person("aa", 22))
    val df: DataFrame = persons.toDF()
    //df.map( (row: Row) => Row(row.get(0), row.getAs[Int](1) * 10) )(RowEncoder.apply(df.schema)).show()
    val ds: Dataset[Person] = persons.toDS()
    ds.map((person: Person) => Person(person.name, person.age * 2))
      .show()
  }
}

case class Person(name: String, age: Int)
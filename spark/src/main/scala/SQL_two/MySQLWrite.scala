package SQL_two

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, FloatType, IntegerType, StructField, StructType}

/**
 * description: MySQLWrite 
 * date: 2020/6/20 12:40 
 * author: nogc
 * version: 1.0 
 */
object MySQLWrite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("mx")
      .getOrCreate()
    val schema = StructType(
      List(

        StructField("age",IntegerType),
        StructField("gpa",FloatType)
      )
    )
  }
}

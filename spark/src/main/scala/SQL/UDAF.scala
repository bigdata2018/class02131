package SQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

/**
 * description: UDAF
 * date: 2020/6/12 11:20
 * author: nogc
 * version: 1.0
 */
/*object UDAF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("hello")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._


    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))


  }
}
class MyAC extends AccumulatorV2[Int,Int]{
  var sum:Int = 0
  var count:Int = 0

  override def isZero: Boolean = {
    return sum==0 && count==0
  }

  override def copy(): AccumulatorV2[Nothing, Nothing] = {
    val newMyAc = new MyAC
    newMyAc.sum = this.sum
    newMyAc.count = this.count
    newMyAc
  }

  override def reset(): Unit = {
    sum =0
    count = 0
  }

  override def add(v: Nothing): Unit = {
    sum += v
    count += 1
  }

  override def merge(other: AccumulatorV2[Nothing, Nothing]): Unit ={
    other match {
      case o:MyAC=>{
        sum += o.sum
        count += o.count
      }
      case _=>sum/count
    }
  }

  override def value: Nothing = ???
}*/

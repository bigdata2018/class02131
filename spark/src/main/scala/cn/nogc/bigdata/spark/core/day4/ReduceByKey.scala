package cn.nogc.bigdata.spark.core.day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: ReduceByKey 
 * date: 2020/6/5 14:59 
 * author: nogc
 * version: 1.0 
 */
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)


    // TODO Scala - 转换算子 - groupByKey
    var rdd = sc.makeRDD(
      List(
        ("hello", 1),
        ("hello", 2),
        ("hadoop", 2)
      )
    )

    // 使用key进行分组操作
   val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
  // val value: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    /*val rdd2 = rdd1.mapValues(
      datas => {
        datas.sum
      }
    )*/
    val value: RDD[(String, Int)] = rdd1.map(t => {
      (t._1, t._2.sum)
    })
    value.collect().foreach(println)

    sc.stop
  }
}

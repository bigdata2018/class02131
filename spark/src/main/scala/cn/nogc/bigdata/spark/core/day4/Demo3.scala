package cn.nogc.bigdata.spark.core.day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: Demo3 
 * date: 2020/6/5 10:28 
 * author: nogc
 * version: 1.0 
 */
object Demo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val rdd1: RDD[Int] = rdd.coalesce(2)
    val rdd2: RDD[Int] = rdd.coalesce(2, true)

    val rdd3: RDD[(Int, Int)] = rdd2.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          d => {
            (index, d)
          }
        )
      }
    )
    rdd3.collect().foreach(println)
    sc.stop




  }
}

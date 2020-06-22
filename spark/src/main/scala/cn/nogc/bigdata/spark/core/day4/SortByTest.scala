package cn.nogc.bigdata.spark.core.day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: SortByTest 
 * date: 2020/6/5 11:06 
 * author: nogc
 * version: 1.0 
 */
object SortByTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2))
    val rdd1: RDD[Int] = rdd.sortBy(num => num)
    val rdd2: RDD[Int] = rdd.sortBy(num => num,false)
    println(rdd.collect().mkString(","))
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    sc.stop()
  }
}

package cn.nogc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: RDD01 
 * date: 2020/6/3 15:31 
 * author: nogc
 * version: 1.0 
 */
object RDD01 {
  def main(args: Array[String]): Unit = {
    val cc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cc")
    val sc = new SparkContext(cc)
    val value: RDD[String] = sc.textFile("input/apache.log")
    val value1: RDD[String] = value.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )
    value1.collect().foreach(println)
    sc.stop()
  }
}

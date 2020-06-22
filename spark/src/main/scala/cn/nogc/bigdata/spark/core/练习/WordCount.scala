package cn.nogc.bigdata.spark.core.练习

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: WordCount 
 * date: 2020/6/11 14:19 
 * author: nogc
 * version: 1.0 
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("input/word.txt")
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word =>
        (word, 1)
    )
    /*val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(k => (k._1))
    val result: RDD[(String, Int)] = groupRDD.map {
      case (k, v) => {
        val ints: Iterable[Int] = v.map(_._2)
        (k, ints.sum)
      }
    }*/

    val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    result.collect.foreach(println)


    sc.stop()
  }
}

package cn.nogc.bigdata.spark.core.day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: Spark07_RDD_Par 
 * date: 2020/6/4 8:44 
 * author: nogc
 * version: 1.0 
 */
object Spark07_RDD_Par {
  def main(args: Array[String]): Unit = {
    val cc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cc")
    //cc.set("spark.default.parallelism","6")
    val context = new SparkContext(cc)
    /*def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }*/
    //（0+1）*5/3
    val rdd: RDD[Int] = context.makeRDD(List(1, 2, 3, 4,5),3)
    println(rdd.partitions.length)
    rdd.saveAsTextFile("output")
    context.stop()
  }
}

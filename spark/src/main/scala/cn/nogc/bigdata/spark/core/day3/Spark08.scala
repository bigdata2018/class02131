package cn.nogc.bigdata.spark.core.day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: Spark07_RDD_Par
 * date: 2020/6/4 8:44
 * author: nogc
 * version: 1.0
 */
object Spark08 {
  def main(args: Array[String]): Unit = {
    val cc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cc")
    //cc.set("spark.default.parallelism","6")
    val context = new SparkContext(cc)
    //7byte/2=3
    /*long goalSize = totalSize / (long)(numSplits == 0 ? 1 : numSplits);*/
    //3+3+1=3分区
    val rdd: RDD[String] = context.textFile("input/word.txt",2)
    rdd.saveAsTextFile("output")
    context.stop()
  }
}

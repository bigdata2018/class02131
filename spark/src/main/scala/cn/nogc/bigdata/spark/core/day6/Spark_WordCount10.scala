package cn.nogc.bigdata.spark.core.day6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * description: Spark_WordCount10 
 * date: 2020/6/8 9:03 
 * author: nogc
 * version: 1.0 
 */
object Spark_WordCount10 {
  def main(args: Array[String]): Unit = {
    //1.1 准备Spark环境
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //1.2 获取上下文环境对象
    val sc = new SparkContext(conf)
    //2.1 读文件中数据
    val fileRDD: RDD[String] = sc.textFile("input")
    // TODO: 1,groupBy

    // TODO: 2,groupByKey

    // todo : 3,

    sc.stop()
  }
}

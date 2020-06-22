package cn.nogc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: Spark02_WordCount 
 * date: 2020/5/29 18:05 
 * author: nogc
 * version: 1.0 
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //1.获取上下文环境对象
    val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)


    val result: Unit = sc
      .textFile("input")
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(_._1)
      .map {
        case (word, list) => {
          (word, list.size)
        }
      }
      .collect()
      .foreach(println)

    //3释放连接
    sc.stop()
  }
}


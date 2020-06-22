package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * description: WordCount10_mapValues
 * date: 2020/6/7 17:29 
 * author: nogc
 * version: 1.0 
 */
object WordCount10_mapValues {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2、创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(conf)
    //3、读取文件数据
    val rdd1 = sc.makeRDD(Array("c","c","a","b","a"))
    //4、数据处理
    val rdd2: RDD[String] = rdd1.flatMap(_.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map((_, 1))
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupBy(_._1)
    val result: RDD[(String, Int)] = rdd4.mapValues(_.foldLeft(0)(_ + _._2))
    //5、打印
    result.collect().foreach(println)
    //6、关闭连接
    sc.stop()
  }
}

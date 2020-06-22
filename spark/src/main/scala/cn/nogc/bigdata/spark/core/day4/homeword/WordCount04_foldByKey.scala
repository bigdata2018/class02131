package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * description: WordCount04_foldByKey 
 * date: 2020/6/5 20:55 
 * author: nogc
 * version: 1.0 
 */
object WordCount04_foldByKey {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2、创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(conf)
    //3、读取文件数据
    val rdd: RDD[String] = sc.textFile("input/word.txt")
    //4、分词
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))
    //5、转换结构
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    //6、
    val result: Array[(String, Int)] = rdd2.foldByKey(0)(
      (x, y) => x + y
    ).collect()
    //7、打印结构
    result.foreach(println)
    //8、关闭连接
    sc.stop()
  }
}
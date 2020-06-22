package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * description: WordCount07_fold 
 * date: 2020/6/7 16:40 
 * author: nogc
 * version: 1.0 
 */
object WordCount07_cogroup {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2、创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(conf)
    //3、读取文件数据
    val rdd1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("a", 5), ("b", 2), ("b", 6), ("c", 3), ("d", 2)))
    val rdd2 = sc.parallelize(Seq(("a", 10), ("b", 1), ("d", 3)))
    val rdd3 = sc.parallelize(Seq(("b", 10), ("a", 1)))
    //4、多个 RDD 协同分组, 将多个 RDD 中 Key 相同的 Value 分组
    val result1: Array[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2).collect()
    val result2: Array[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2, rdd3).collect()
    //5、打印结构
   // result1.foreach(println)
    result2.foreach(println)
    //6、关闭连接
    sc.stop()
  }
}

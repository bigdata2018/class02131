package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * description: FoleByKey 
 * date: 2020/6/5 18:54 
 * author: nogc
 * version: 1.0 
 */
object FoleByKey {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    //2、创建Spark上下文环境对象
    val sc = new SparkContext(conf)
    //3、读取数据
    val rdd1: RDD[(String, Int)] = sc.makeRDD(Seq(("a",1), ("a",1), ("b",1)))
    //4、可以指定初始值10
    val result: RDD[(String, Int)] = rdd1.foldByKey(zeroValue = 10)((curr, agg) => curr + agg)
    //5、打印结果
    result.collect().foreach(println)
    //6、关闭Spark连接
    sc.stop()
  }
}

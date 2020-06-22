package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: WordCount09_countByValue 
 * date: 2020/6/7 17:20 
 * author: nogc
 * version: 1.0 
 */
object WordCount09_countByValue {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //2、创建Spark上下文环境对象（连接对象）
    val sc : SparkContext = new SparkContext(conf)
    //3、读取文件数据
    val rdd1 = sc.makeRDD(Array("c","c","a","b","a"))
    //4、求得整个数据集中 Key 以及对应 Key 出现的次数
    val result = rdd1.countByValue()
    //5、打印
    println(result)
    //6、关闭连接
    sc.stop()
  }
}

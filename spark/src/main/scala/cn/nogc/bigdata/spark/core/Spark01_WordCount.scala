package cn.nogc.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: Spark01_WordCount 
 * date: 2020/5/29 17:41 
 * author: nogc
 * version: 1.0 
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //1.1 准备Spark环境
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //1.2 获取上下文环境对象
    val sc = new SparkContext(conf)

    //2.1 读文件中数据
    val fileRDD: RDD[String] = sc.textFile("input")
    //2.2 分词/扁平化
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    //2.3 拆分后的数据结构改变
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
  /*  //2.4 分组
    val word2IterRdd: RDD[(String, Iterable[(String, Int)])] = word2OneRDD.groupBy(_._1)
    //2.5 聚合
    val word2CountRDD: RDD[(String, Int)] = word2IterRdd.map {
      case (word, list) => {
        (word, list.size)
      }
    }*/

    val value: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)
    //2.6 展示
    val result: Array[(String, Int)] = value.collect()
    result.foreach(println)

    //3释放连接
    sc.stop()
  }
}

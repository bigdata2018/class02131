package cn.nogc.bigdata.spark.core.day6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * description: Mappartitions 
 * date: 2020/6/8 8:38 
 * author: nogc
 * version: 1.0 
 */
object Mappartitions {
  def main(args: Array[String]): Unit = {
    //1.1 准备Spark环境
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //1.2 获取上下文环境对象
    val sc = new SparkContext(conf)
    //2.1 读文件中数据
    val value: RDD[Int] = sc.makeRDD(List(1, 1, 2, 2), 2)
    val result: RDD[(Int, Int)] = value.mapPartitions(
      iter => {
        val len = iter.length
        //iter.hasNext
        List((iter.next(), len)).iterator
      }
    )
    //println(result.collect().mkString(","))
    sc.stop()


  }
}

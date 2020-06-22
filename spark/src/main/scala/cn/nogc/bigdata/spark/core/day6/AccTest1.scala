package cn.nogc.bigdata.spark.core.day6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * description: AccTest1 
 * date: 2020/6/8 15:01 
 * author: nogc
 * version: 1.0 
 */
object AccTest1 {
  def main(args: Array[String]): Unit = {
    //1.1 准备Spark环境
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //1.2 获取上下文环境对象
    val sc = new SparkContext(conf)
    //2.1 读文件中数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    //2.2 声明累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val result: Unit = rdd.foreach(
      num => {
        //2.3 声明累加器
        sum.add(num)
      }
    )
    //3 打印结果
    println("sum = "+ sum.value)
    //4 释放连接
    sc.stop()
  }
}

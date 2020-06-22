package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: AggregateByKey 
 * date: 2020/6/5 19:11
 * author: nogc
 * version: 1.0 
 */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    //2、创建Spark上下文环境对象
    val sc = new SparkContext(conf)
    //3、读取数据
    val rdd1: RDD[(String, Double)] = sc.makeRDD(Seq(("手机",10.0), ("手机",15.0), ("电脑",20.0)))
    //4、zeroValue 初始值
    //seqOp 转换每一个值的函数
    //comboOp 将转换过的值聚合的函数
    val result: Array[(String, Double)] = rdd1.aggregateByKey(0.8)(
      //seqOp = (zero, price) => price * zero,
      //combOp = (curr, agg) => curr + agg
      (x, y) => x*y,
      (x, y) => x + y
    ).collect()
    //5、打印结果
    result.foreach(println)
    //6、关闭Spark连接
    sc.stop()
  }
}

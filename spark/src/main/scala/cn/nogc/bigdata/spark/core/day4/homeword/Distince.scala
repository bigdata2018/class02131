package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * description: Distince 
 * date: 2020/6/5 19:49 
 * author: nogc
 * version: 1.0 
 */
object Distince {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    //2、创建Spark上下文环境对象
    val sc = new SparkContext(conf)
    //3、读取数据
    val rdd1: RDD[Int] = sc.makeRDD(Seq(1,7,1,5,5,5,2,2,3))
    /*//4、distinct方式去重
    val result: RDD[Int] = rdd1
      .distinct()
    */
    val rdd2: RDD[(Int, Int)] = rdd1.map(x => (x, 1))
    val rdd3: RDD[(Int, Int)] = rdd2.reduceByKey((x, y) => x, 1)
    val result: RDD[Int] = rdd3.map(_._1)
    //5、打印结果
    result.collect().foreach(println)
    //6、关闭Spark连接
    sc.stop()
  }
}

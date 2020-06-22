package cn.nogc.bigdata.spark.core.day4.homeword

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * description: CombineByKey 
 * date: 2020/6/5 19:21
 * author: nogc
 * version: 1.0 
 */
object CombineByKey {
  def main(args: Array[String]): Unit = {
    //1、创建Spark运行配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    //2、创建Spark上下文环境对象
    val sc = new SparkContext(conf)
    //3、读取数据
    val rdd1: RDD[(String, Double)] = sc.makeRDD(List(("zhangsan",99.0),
      ("zhangsan",96.0),
      ("lisi",97.0),
      ("lisi",98.0),
      ("zhangsan",97.0)))
    //4、对数据集按照 Key 进行聚合
    val combineRDD: RDD[(String, (Double, Int))] = rdd1.combineByKey(
      score => (score, 1),
      (scoreCount: (Double, Int), newScore) => (scoreCount._1 + newScore, scoreCount._2 + 1),
      (scoreCount1: (Double, Int), scoreCount2: (Double, Int)) => (scoreCount1._1 + scoreCount2._1, scoreCount1._2 + scoreCount2._2)
    )

    val result: RDD[(String, Double)] = combineRDD.map(score => {
      (score._1, score._2._1 / score._2._2)
    })
    //5、打印结果
    result.collect() foreach (println)
    //6、关闭Spark连接
    sc.stop()
  }
}

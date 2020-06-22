package cn.nogc.bigdata.spark.core.day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: Spark_top3 
 * date: 2020/6/6 11:16 
 * author: nogc
 * version: 1.0 
 */
object Spark_top3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)
    //1 读数据
    val fileRDD: RDD[String] = sc.textFile("input/agent.log")
    //2 结构转换
    val mapRDD: RDD[(String, Int)] = fileRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    )
    //3、
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    //4
    val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
      case (k, cnt) => {
        val ks = k.split("-")
        (ks(0), (ks(1), cnt))
      }
    }
    //5
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()
    //6
    val result: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    )

    //7
    result.collect().foreach(println)

    sc.stop()
  }
}

package cn.nogc.bigdata.spark.core.day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: CoalesceTest2 
 * date: 2020/6/5 10:36 
 * author: nogc
 * version: 1.0 
 */
object CoalesceTest2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)
    var rdd = sc.makeRDD(List(1,2,3,4,5,6),3)
    val rdd1: RDD[Int] = rdd.coalesce(4, true)
    val rdd3 = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          d => {
            (index, d)
          }
        )
      }
    )
    //rdd3.saveAsTextFile("output")
    rdd3.collect().foreach(println)



    sc.stop
  }
}

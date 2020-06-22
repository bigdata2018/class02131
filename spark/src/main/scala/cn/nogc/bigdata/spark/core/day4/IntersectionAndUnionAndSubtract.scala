package cn.nogc.bigdata.spark.core.day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: IntersectionAndUnionAndSubtract 
 * date: 2020/6/5 11:16 
 * author: nogc
 * version: 1.0 
 */
object IntersectionAndUnionAndSubtract {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)


    // TODO Scala - 转换算子

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))
    val rdd6 = sc.makeRDD(List("3", "4", "5", "6"))

    // 交集，并集，差集调用时所传递的RDD数据类型要和当前RDD的数据类型一致

    // 交集
    val rdd3 = rdd1.intersection(rdd2)
    //val rdd7 = rdd1.intersection(rdd6)

    // 并集
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    //val rdd8: RDD[Int] = rdd1.union(rdd6)

    // 差集
    val rdd5 = rdd1.subtract(rdd2)
    //val rdd9 = rdd1.subtract(rdd6)

    //拉链
    val rdd7: RDD[(Int, Int)] = rdd1.zip(rdd2)


    println(rdd3.collect().mkString(","))
    println(rdd4.collect().mkString(","))
    println(rdd5.collect().mkString(","))
    println(rdd7.collect().mkString(","))
    sc.stop
  }
}
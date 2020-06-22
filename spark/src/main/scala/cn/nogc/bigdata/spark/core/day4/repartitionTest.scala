package cn.nogc.bigdata.spark.core.day4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * description: repartitionTest 
 * date: 2020/6/5 10:43 
 * author: nogc
 * version: 1.0 
 */
object repartitionTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new SparkContext(conf)


    // TODO Scala - 转换算子 - repartition - 改变分区
    val rdd = sc.makeRDD(List(1,2,3,4,5,6), 3)

    val rdd2: RDD[Int] = rdd.repartition(4)

    val rdd3 = rdd2.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map(
          d => {
            (index, d)
          }
        )
      }
    )

    rdd3.collect().foreach(println)



    sc.stop
  }
}

package SQL_two

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * description: N1
 * date: 2020/6/19 15:44
 * author: nogc
 * version: 1.0
 */
object N1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("N1")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("input")
    val flatMapRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map((_, 1))
    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    val result: Array[(String, Int)] = reduceByKeyRDD.collect()
    result.foreach(println)
    sc.stop()
  }
}

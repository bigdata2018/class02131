package cn.nogc.bigdata.spark.core.day6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * description: AccTest3_AccumulatorV2 
 * date: 2020/6/8 15:13 
 * author: nogc
 * version: 1.0 
 */
object AccTest3_AccumulatorV2 {
  def main(args: Array[String]): Unit = {
    //1.1 准备Spark环境
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //1.2 获取上下文环境对象
    val sc = new SparkContext(conf)
    //2.1 读文件中数据
    val rdd: RDD[String] = sc.makeRDD(List("hi spark","hi flink","hi nogc"))
    //2.2 创建累加器
    val acc = new WordCountAccumulator
    //2.3 注册累加器
    sc.register(acc,"wordcount")
    //2.4 使用累加器
    val result: Unit = rdd.foreach(
      words => {
        val w: Array[String] = words.split(" ")
        w.foreach(
          word => {
            acc.add(word)
          }
        )
      }
    )
    //3 打印结果
    println(acc.value)
    //4 释放连接
    sc.stop()
  }

  //1 自定义累加器
  class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]]{
    var innerMap = mutable.Map[String, Int]()

    // TODO 累加器是否初始化
    // Z
    override def isZero: Boolean = !innerMap.isEmpty

    // TODO 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new WordCountAccumulator
    }

    // TODO 重置累加器
    override def reset(): Unit = {
      innerMap.clear()
    }

    // TODO 累加数据
    override def add(word: String): Unit = {
      val cnt = innerMap.getOrElse(word, 0)
      innerMap.update(word, cnt + 1)
    }

    // TODO 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      // 两个Map的合并
      var map1 = this.innerMap
      var map2 = other.value

      innerMap = map1.foldLeft(map2)(
        (map, kv) => {
          val k = kv._1
          val v = kv._2
          map(k) = map.getOrElse(k, 0) + v
          map
        }
      )
    }

    // TODO 获取累加器的值，就是累加器的返回结果
    override def value: mutable.Map[String, Int] = innerMap
  }
}
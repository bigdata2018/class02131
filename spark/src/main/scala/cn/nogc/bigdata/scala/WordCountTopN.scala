package cn.nogc.bigdata.scala

import scala.io.Source

/**
 * description: WordCountTopN 
 * date: 2020/5/29 21:02 
 * author: nogc
 * version: 1.0 
 */
object WordCountTopN {
  def main(args: Array[String]): Unit = {
    val list: List[String] = Source.fromFile("input/word.txt").getLines().toList
    //println(list)
    val strings: List[String] = list.flatMap(_.split(" "))
    //println(strings)
    val tuples: List[(String, Int)] = strings.map((_,1))
    //println(tuples)
    val stringToTuples: Map[String, List[(String, Int)]] = tuples.groupBy(_._1)
    println(stringToTuples)

    val stringToInt: Map[String, Int] = stringToTuples.map(
      kv => {
        (kv._1, kv._2.size)
      }
    )
    println(stringToInt)

    val tuples1: List[(String, Int)] = stringToInt.toList.sortWith((left, right) => left._2 > right._2)
    val tuples2: List[(String, Int)] = tuples1.take(2)
    println(tuples2)
  }
}

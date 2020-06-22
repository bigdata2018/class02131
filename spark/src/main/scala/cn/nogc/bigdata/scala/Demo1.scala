package cn.nogc.bigdata.scala

/**
 * description: Demo1 
 * date: 2020/5/30 8:54 
 * author: nogc
 * version: 1.0 
 */

object Demo1 {
  def main(args: Array[String]): Unit = {
/*    val list = List(("hello", 4),
      ("hello spark", 3),
      ("hello spark scala", 2),
      ("hello spark scala hive", 1)
    )
    val strings: List[String] = list.map(t => {
      val value: String = t._1
      val value1: Int = t._2
      (value + " ") * value1
    })
    //println(strings)
    val strings1: List[String] = strings.flatMap(_.split(" "))
    val tuples: List[(String, Int)] = strings1.map((_, 1))
//    println(tuples)

    val stringToTuples: Map[String, List[(String, Int)]] = tuples.groupBy(_._1)
    //println(stringToTuples)
    val stringToInt: Map[String, Int] = stringToTuples.map(
      t => {
        (t._1, t._2.size)

    //println(stringToInt)
    val tuples1: List[(String, Int)] = stringToInt.toList.sortWith((a1, a2) => a1._2 > a2._2)
    val tuples2: List[(String, Int)] = tuples1.take(3)
    println(tuples2)
  })*/
}
}
package cn.nogc.bigdata.scala

/**
 * description: Reduce 
 * date: 2020/5/29 20:32 
 * author: nogc
 * version: 1.0 
 */
object Reduce {
  def main(args: Array[String]): Unit = {
    /*reduce表示将列表，传入一个函数进行聚合计算
    1. 定义一个列表，包含以下元素：1,2,3,4,5,6,7,8,9,10
    2. 使用reduce计算所有元素的和*/

    val ints = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    //val result1: Int = ints.reduce((x, y) => x + y)

    //val result2: Int = ints.reduce(_ + _)

    //val result3: Int = ints.reduceLeft(_ + _)

    val reslut4: Int = ints.reduceRight(_ + _)
    println(reslut4)
  }
}

package cn.nogc.bigdata.scala

/**
 * description: Flod 
 * date: 2020/5/29 20:39 
 * author: nogc
 * version: 1.0 
 */
object Flod {
  def main(args: Array[String]): Unit = {
    /* 1. 定义一个列表，包含以下元素：1,2,3,4,5,6,7,8,9,10
       2. 使用fold方法计算所有元素的和*/
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    //val i: Int = list.fold(0)(_ + _)

    val i: Int = list.fold(2)(_ + _)
    //println(i)

    val i1: Int = list.foldLeft(2)(_ + _)
    //println(i1)

    val i2: Int = list.foldRight(2)(_ + _)
    println(i2)
  }
}

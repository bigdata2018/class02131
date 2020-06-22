package atguigu.bigdata.spark.core.req.controller

import atguigu.bigdata.spark.core.req.service.WordCountService
import com.atguigu.summer.framework.core.TController


/**
  * WordCount控制器
  */
class WordCountController extends TController{

   /* private val wordCountService = new WordCountService

    override def execute(): Unit = {
        val wordCountArray: Array[(String, Int)] = wordCountService.analysis()
        println(wordCountArray.mkString(","))
    }*/

    private val wordCountService = new WordCountService

    override def execute(): Unit = {
        val wordCountArray: Array[(String, Int)] = wordCountService.analysis()
        println(wordCountArray.mkString(","))
    }
}

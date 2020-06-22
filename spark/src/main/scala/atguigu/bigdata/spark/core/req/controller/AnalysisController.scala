package atguigu.bigdata.spark.core.req.controller

import atguigu.bigdata.spark.core.req.service.AnalysisService
import com.atguigu.summer.framework.core.TController

/**
 * description: AnalysisController 
 * date: 2020/6/9 17:35 
 * author: nogc
 * version: 1.0 
 */
class AnalysisController extends TController{
  private val analysisService = new AnalysisService
  override def execute(): Unit = {
    val result: Array[(String, (Int, Int, Int))] = analysisService.analysis()
    result.foreach(println)
  }
}

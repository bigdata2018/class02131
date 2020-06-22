package atguigu.bigdata.spark.core.req.application

import atguigu.bigdata.spark.core.req.application.HotCategoryAnalysisTop10Application.start
import atguigu.bigdata.spark.core.req.controller.{AnalysisController, HotCategoryAnalysisTop10Controller}
import com.atguigu.summer.framework.core.TApplication

/**
 * description: Analysis 
 * date: 2020/6/9 19:53 
 * author: nogc
 * version: 1.0 
 */
object Analysis extends App with TApplication {
  start("spark") {
    val controller = new AnalysisController
    controller.execute()
  }
}

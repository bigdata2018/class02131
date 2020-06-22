package atguigu.bigdata.spark.core.req.controller

import atguigu.bigdata.spark.core.req.service.PageflowService
import com.atguigu.summer.framework.core.TController

/**
 * description: PageflowController 
 * date: 2020/6/10 11:22 
 * author: nogc
 * version: 1.0 
 */
class PageflowController extends TController{

  private val pageflowService = new PageflowService

  override def execute(): Unit = {
    val result = pageflowService.analysis()
  }
}

package atguigu.bigdata.spark.core.req.application

import atguigu.bigdata.spark.core.req.controller.PageflowController
import com.atguigu.summer.framework.core.TApplication

/**
 * description: PageflowApplication 
 * date: 2020/6/10 11:23 
 * author: nogc
 * version: 1.0 
 */
object PageflowApplication extends App with TApplication{

  start( "spark" ) {
    val controller = new PageflowController
    controller.execute()
  }
}

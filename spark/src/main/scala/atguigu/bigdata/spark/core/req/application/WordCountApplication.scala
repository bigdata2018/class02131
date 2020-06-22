package atguigu.bigdata.spark.core.req.application

import atguigu.bigdata.spark.core.req.controller.WordCountController

import com.atguigu.summer.framework.core.TApplication


object WordCountApplication extends App with TApplication{

  /*  start( "spark" ) {

        val controller = new WordCountController
        controller.execute()

    }*/
    start("spark"){
        val controller = new WordCountController
        controller.execute()
    }

}

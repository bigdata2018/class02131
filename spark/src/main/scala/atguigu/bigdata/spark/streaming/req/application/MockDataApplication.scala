package atguigu.bigdata.spark.streaming.req.application

import atguigu.bigdata.spark.streaming.req.controller.MockDataController
import com.atguigu.summer.framework.core.TApplication

object MockDataApplication extends App with TApplication{

    start("sparkStreaming") {
        val controller = new MockDataController
        controller.execute()
    }
}

package atguigu.bigdata.spark.core.req.service

import atguigu.bigdata.spark.core.req.dao.HotCategoryAnalysisTop10Dao
import com.atguigu.summer.framework.core.TService
import org.apache.spark.rdd.RDD

class HotCategoryAnalysisTop10Service extends TService{

    private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao

    /**
      * 数据分析
      *
      * @return
      */
    override def analysis() = {

        // TODO 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")

        // TODO 对品类进行点击的统计
        //line =>
        //    click = (1, 0, 0)
        //    order = (0, 1, 0)
        //    pay   = (0, 0, 1)

        val flatMapRDD = actionRDD.flatMap(
            action => {
                val datas = action.split("_")
                if ( datas(6) != "-1" ) {
                    // 点击的场合
                    List( (datas(6), (1,0,0)) )
                } else if ( datas(8) != "null" ) {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map(id=>(id, (0,1,0)))
                } else if ( datas(10) != "null" ) {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map(id=>(id, (0,0,1)))
                } else {
                    Nil
                }
            }
        )

        val reduceRDD = flatMapRDD.reduceByKey(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )

        reduceRDD.sortBy(_._2, false).take(10)
    }

    /**
      * 数据分析
      *
      * @return
      */
    def analysis2() = {

        // TODO 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")
        actionRDD.cache()

        // TODO 对品类进行点击的统计
        //line => （category，clickCount）
        // （品类1， 10）
        val clickRDD = actionRDD.map(
            action => {
                val datas = action.split("_")
                (datas(6), 1)
            }
        ).filter( _._1 != "-1" )

        val categoryIdToClickCountRDD = clickRDD.reduceByKey(_+_)

        // TODO 对品类进行下单的统计
        //（category，orderCount）
        // （品类1，品类2，品类3， 10）
        // （品类1，10）， （品类2，10）， （品类3， 10）
        val orderRDD = actionRDD.map(
            action => {
                val datas = action.split("_")
                datas(8)
            }
        ).filter( _ != "null" )

        val orderToOneRDD = orderRDD.flatMap{
            id => {
                val ids = id.split(",")
                ids.map( id=>(id,1) )
            }
        }
        val categoryIdToOrderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_+_)

        // TODO 对品类进行支付的统计
        //（category，payCount）
        val payRDD = actionRDD.map(
            action => {
                val datas = action.split("_")
                datas(10)
            }
        ).filter( _ != "null" )

        val payToOneRDD = payRDD.flatMap{
            id => {
                val ids = id.split(",")
                ids.map( id=>(id,1))
            }
        }
        val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_+_)

        // TODO 将上面统计的结果转换结构
        // tuple => ( 元素1， 元素2， 元素3 )
        // （品类，点击 1），（品类，下单 1），（品类，支付1）
        // （品类，点击数量），（品类，下单数量），（品类，支付数量）
        // （品类，(点击数量,0,0)）, （品类，(0, 下单数量,0）），（品类，（0，0，支付数量））
        // ( 品类， （点击数量，下单数量，支付数量） )
        // 1,2,3 = 4
        val newCategoryIdToClickCountRDD = categoryIdToClickCountRDD.map{
            case ( id, clickCount ) => {
                ( id, (clickCount, 0, 0) )
            }
        }
        val newCategoryIdToOrderCountRDD = categoryIdToOrderCountRDD.map{
            case ( id, orderCount ) => {
                ( id, (0, orderCount, 0) )
            }
        }
        val newCategoryIdToPayCountRDD = categoryIdToPayCountRDD.map{
            case ( id, payCount ) => {
                ( id, (0, 0, payCount) )
            }
        }
        val countRDD = newCategoryIdToClickCountRDD.union(newCategoryIdToOrderCountRDD).union(newCategoryIdToPayCountRDD)
        val reduceCountRDD = countRDD.reduceByKey(
            ( t1, t2 ) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )

        // TODO 将转换结构后的数据进行排序（降序）
        val sortRDD: RDD[(String, (Int, Int, Int))] = reduceCountRDD.sortBy(_._2, false)

        // TODO 将排序后的结果取前10名
        val result = sortRDD.take(10)

        result
    }

    /**
      * 数据分析
      *
      * @return
      */
    def analysis1() = {

        // TODO 读取电商日志数据
        val actionRDD: RDD[String] = hotCategoryAnalysisTop10Dao.readFile("input/user_visit_action.txt")

        // TODO 对品类进行点击的统计
        //line => （category，clickCount）
        // （品类1， 10）
        val clickRDD = actionRDD.map(
            action => {
                val datas = action.split("_")
                (datas(6), 1)
            }
        ).filter( _._1 != "-1" )

        val categoryIdToClickCountRDD = clickRDD.reduceByKey(_+_)

        // TODO 对品类进行下单的统计
        //（category，orderCount）
        // （品类1，品类2，品类3， 10）
        // （品类1，10）， （品类2，10）， （品类3， 10）
        val orderRDD = actionRDD.map(
            action => {
                val datas = action.split("_")
                datas(8)
            }
        ).filter( _ != "null" )

        val orderToOneRDD = orderRDD.flatMap{
            id => {
                val ids = id.split(",")
                ids.map( id=>(id,1) )
            }
        }
        val categoryIdToOrderCountRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_+_)

        // TODO 对品类进行支付的统计
        //（category，payCount）
        val payRDD = actionRDD.map(
            action => {
                val datas = action.split("_")
                datas(10)
            }
        ).filter( _ != "null" )

        val payToOneRDD = payRDD.flatMap{
            id => {
                val ids = id.split(",")
                ids.map( id=>(id,1) )
            }
        }
        val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_+_)

        // TODO 将上面统计的结果转换结构
        // tuple => ( 元素1， 元素2， 元素3 )
        // （品类，点击数量），（品类，下单数量），（品类，支付数量）
        // ( 品类， （点击数量，下单数量，支付数量） )
        val joinRDD: RDD[(String, (Int, Int))] = categoryIdToClickCountRDD.join( categoryIdToOrderCountRDD )
        val joinRDD1: RDD[(String, ((Int, Int), Int))] = joinRDD.join( categoryIdToPayCountRDD )
        val mapRDD: RDD[(String, (Int, Int, Int))] = joinRDD1.mapValues {
            case ((clickCount, orderCount), payCount) => {
                (clickCount, orderCount, payCount)
            }
        }

        // TODO 将转换结构后的数据进行排序（降序）
        val sortRDD: RDD[(String, (Int, Int, Int))] = mapRDD.sortBy(_._2, false)

        // TODO 将排序后的结果取前10名
        val result = sortRDD.take(10)

        result
    }
}

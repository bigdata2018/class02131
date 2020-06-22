package atguigu.bigdata.spark.core.req.service


import atguigu.bigdata.spark.core.req.dao.AnalysisDao
import com.atguigu.summer.framework.core.TService
import org.apache.spark.rdd.RDD

/**
 * description: AnalysisService 
 * date: 2020/6/9 17:34 
 * author: nogc
 * version: 1.0 
 */
class AnalysisService extends TService {
  /**
   * 数据分析
   *
   * @return
   */
  private val analysisDao = new AnalysisDao

 override def analysis(): Array[(String, (Int, Int, Int))] = {
    val fileRDD: RDD[String] = analysisDao.readFile("input/user_visit_action.txt")
    // 对品类进行点击的统计
    val flatMapRDD: RDD[(String, (Int, Int, Int))] = fileRDD.flatMap(
      words => {
        val word: Array[String] = words.split("_")
        if (word(6) != "-1") {
          List((word(6), (1, 0, 0)))
        } else if (word(8) != "null") {
          val strings: Array[String] = word(8).split(",")
          strings.map(t => (t, (0, 1, 0)))
        } else if (word(10) != "null") {
          val strings: Array[String] = word(10).split(",")
          strings.map(t => (t, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val reduceByKeyRDD: RDD[(String, (Int, Int, Int))] = flatMapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    reduceByKeyRDD.sortBy(_._2, false).take(10)

  }

  def analysis2() = {
    val fileRDD: RDD[String] = analysisDao.readFile("input/user_visit_action.txt")
    // 对品类进行点击的统计
    fileRDD.cache()
    val mapRDD: RDD[(String, Int)] = fileRDD.map(
      ac => {
        val datas: Array[String] = ac.split("_")
        (datas(6), 1)
      }
    )
    val filterRDD: RDD[(String, Int)] = mapRDD.filter(_._1 != -1)
    val reduceByKey: RDD[(String, Int)] = filterRDD.reduceByKey(_ + _)

    val orderRDD: RDD[String] = fileRDD.map(
      ac => {
        val datas: Array[String] = ac.split("_")
        datas(8)
      }
    ).filter(_ != "null")
    val value: RDD[(String, Int)] = orderRDD.flatMap {
      id => {
        val ids = id.split(",")
        ids.map(id => (id, 1))
      }
    }
    val value1: RDD[(String, Int)] = value.reduceByKey(_ + _)

    val payRDD: RDD[String] = fileRDD.map(
      ac => {
        val datas: Array[String] = ac.split("_")
        datas(10)
      }
    ).filter(_ != "null")
    val payToOneRDD = payRDD.flatMap{
      id => {
        val ids = id.split(",")
        ids.map( id=>(id,1) )
      }
    }
    val categoryIdToPayCountRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_+_)

    val newCategoryIdToClickCountRDD = reduceByKey.map{
      case ( id, clickCount ) => {
        ( id, (clickCount, 0, 0) )
      }
    }
    val newCategoryIdToOrderCountRDD = value1.map{
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

    def analysis1() = {

      // TODO 读取电商日志数据
      val actionRDD: RDD[String] = analysisDao.readFile("input/user_visit_action.txt")

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
}

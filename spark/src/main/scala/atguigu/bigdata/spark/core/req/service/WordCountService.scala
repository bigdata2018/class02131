package atguigu.bigdata.spark.core.req.service

import atguigu.bigdata.spark.core.req.dao.WordCountDao

import com.atguigu.summer.framework.core.TService
import com.atguigu.summer.framework.util.EnvUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountService extends TService {

   /* private val wordCountDao = new WordCountDao

    /**
      * 数据分析
      *
      * @return
      */
    override def analysis() = {
        val fileRDD: RDD[String] = wordCountDao.readFile("input/word.txt")
        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = wordRDD.map( word=>(word,1) )
        val wordToSumRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        val wordCountArray: Array[(String, Int)] = wordToSumRDD.collect()
        wordCountArray
    }*/

    private val wordCountDao = new WordCountDao

    override def analysis() = {
        val wordCountArray: Array[(String, Int)] = wordCountDao
          .readFile("input/word.txt")
          .flatMap(_.split(" "))
          .map(word => (word, 1))
          .reduceByKey(_ + _)
          .collect()
        wordCountArray
    }
}

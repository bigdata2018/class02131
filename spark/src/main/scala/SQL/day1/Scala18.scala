package SQL.day1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * @author chent
 * @create 2020-06-16 14:39
 */
object Scala18 {
  def main(args: Array[String]): Unit = {
    val sparkconf: SparkConf = new SparkConf().setMaster("lcoal[*]").setAppName("Test")
    val ssc = new StreamingContext(sparkconf,Seconds(3))
    ssc.sparkContext.setCheckpointDir("cp")

    val ds= ssc.socketTextStream("localhost",9999)
    ds
      .flatMap(_.split(" "))
        .map((_,1L))
        .updateStateByKey[Long](
          (seq:Seq[Long],buffer:Option[Long])=>{
            val newBufferValue: Long = buffer.getOrElse(0L)+seq.sum
            Option(newBufferValue)
          }
        )
        .print()
    ssc.start()
    ssc.awaitTermination()
  }
}

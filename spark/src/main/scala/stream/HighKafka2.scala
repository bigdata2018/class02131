package stream

/**
 * description: HighKafka2 
 * date: 2020/6/13 10:37 
 * author: nogc
 * version: 1.0 
 */
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
object HighKafka2 {

  def createSSC(): StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))
    // 偏移量保存在 checkpoint 中, 可以从上次的位置接着消费
    ssc.checkpoint("./ck1")
    // kafka 参数
    //kafka参数声明
    val brokers = "nogc112:9092,nogc113:9092,nogc114:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "nogc112:2181,nogc113:2181,nogc114:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))

    dStream.print()
    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", () => createSSC())
    ssc.start()
    ssc.awaitTermination()
  }
}
*/

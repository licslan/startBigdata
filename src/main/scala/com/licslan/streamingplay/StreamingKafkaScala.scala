package com.licslan.streamingplay

import com.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.
{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * @author Weilin Huang
  *         将kafka作为数据源
  **/
object StreamingKafkaScala {

  def main(args: Array[String]): Unit = {

    /**
      * 创建sparkstreaming 环境
      * */
    val sparkStreamingKafka = new SparkConf().setAppName("sparkStreamingKafka")
    val context = new StreamingContext(sparkStreamingKafka,Seconds(2))

    /** 配置连接kafka参数 */
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /** kafka 的 topics */
    val topics = Array("licslan")
    val stream = KafkaUtils.createDirectStream[String, String](
      context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    /** handler data */
    stream.map(record => (record.key, record.value))

    /** 可以异步落地操作
      *
      * 对离线数据进行训练等等  活着实时训练  推测预测
      *
      * */

    context.start()
    context.awaitTermination()
  }

}

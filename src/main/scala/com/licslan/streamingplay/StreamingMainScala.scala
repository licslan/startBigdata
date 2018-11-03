package com.licslan.streamingplay


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamingMainScala {

  def main(args: Array[String]): Unit = {
    val SparkStreaming = new SparkConf()
      /*.setMaster("local[*]")*/
      // no hard code for spark submit
      .setAppName("SparkStreaming")
    val context = new StreamingContext(SparkStreaming, Durations.seconds(2))

    val lines = context.socketTextStream("localhost",9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    context.start()
    context.awaitTermination()

  }

}

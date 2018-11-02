package com.licslan.streamingplay;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
/**
 * @author  Weilin Huang
 * 从控制台读取内容  并进行wordcount demo
 *
 * 提交方式：
 *
 * spark-submit \
 *         --master local[*] \  //no hard code in your code
 *         --class com.icslan.streamingplay.StreamingMain \
 *         --executor-memory 4g \
 *         --executor-cores 4 \
 *         /linux path of jars/scala-1.0-SNAPSHOT.jar
 *
 * */


public class StreamingMain {


    public static void main(final String[] args) throws Exception{

        /**1 创建java spark Streaming context  */
        /**
         * JAVA
         * First, we create a JavaStreamingContext object, which is the main entry point for all streaming functionality.
         * We create a local StreamingContext with two execution threads, and a batch interval of 2 second.
         * */
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf javaSparkStreaming =
                new SparkConf()
                /*.setMaster("local[*]")*/  // no hard code for spark submit
                .setAppName("javaSparkStreaming");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkStreaming, Durations.seconds(2));


        /** 2 利用context  创建一个DStream */
        /**
         * Using this context, we can create a DStream that represents streaming data from a TCP source,
         * specified as hostname (e.g. localhost) and port (e.g. 9999).
         * */
        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);

        /**3 对从网络拿到的数据进行简单处理 空格切割*/
        /**
         * This lines DStream represents the stream of data that will be received from the data server. Each record
         * in this stream is a line of text. Then, we want to split the lines by space into words.
         * */
        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        /**
         * flatMap is a DStream operation that creates a new DStream by generating multiple new records from each record
         * in the source DStream. In this case, each line will be split into multiple words and the stream of words is
         * represented as the words DStream. Note that we defined the transformation using a FlatMapFunction object. As
         * we will discover along the way, there are a number of such convenience classes in the Java API that help defines DStream transformations.
         * Next, we want to count these words.
         * */
        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        /**
         * The words DStream is further mapped (one-to-one transformation) to a DStream of (word, 1) pairs, using a PairFunction object.
         * Then, it is reduced to get the frequency of words in each batch of data, using a Function2 object. Finally, wordCounts.print()
         * will print a few of the counts generated every second.
         *
         * Note that when these lines are executed, Spark Streaming only sets up the computation it will perform after it is started, and
         * no real processing has started yet. To start the processing after all the transformations have been setup, we finally call start method.
         * */

        javaStreamingContext.start();              // Start the computation
        javaStreamingContext.awaitTermination();   // Wait for the computation to terminate


        /**
         * The complete code can be found in the Spark Streaming example JavaNetworkWordCount.
         *
         * If you have already downloaded and built Spark, you can run this example as follows.
         * You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using
         *
         * $ nc -lk 9999
         * Then, in a different terminal, you can start the example by using
         *
         * spark-shell submit
         * the path of class StreamingMain localhost 9999
         * */

    }


}

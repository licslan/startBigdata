package com.licslan.rddplay

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author LICSLAN
  * */
object RddPlay1 {

  /**
    * 本文主要是讲解spark里RDD的基础操作。RDD是spark特有的数据模型，谈到RDD就会提到什么弹性分布式数据集，什么有向无环图，
    * 本文暂时不去展开这些高深概念，在阅读本文时候，大家可以就把RDD当作一个数组，这样的理解对我们学习RDD的API是非常有帮助的。
    * 本文所有示例代码都是使用scala语言编写的。
    *
    * Spark里的计算都是操作RDD进行，那么学习RDD的第一个问题就是如何构建RDD，构建RDD从数据来源角度分为两类：第一类是从内存里直接读取数据，
    * 第二类就是从文件系统里读取，当然这里的文件系统种类很多常见的就是HDFS以及本地文件系统了。
    * */

  def main(args: Array[String]): Unit = {
    /** rdd 基础使用 */

    /**
      * The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster.
      * To create a SparkContext you first need to build a SparkConf object that contains information about your application.
      * Only one SparkContext may be active per JVM. You must stop() the active SparkContext before creating a new one.
      * */


    /** 1 创建sparkcontext  上下文 */
    val conf = new SparkConf().setAppName("rddplay").setMaster("local[4]")
    val sc = new SparkContext(conf)


    /**
      * Resilient Distributed Datasets (RDDs)
      * Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection of
      * elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing collection
      * in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS,
      * HBase, or any data source offering a Hadoop InputFormat.
      * */
    /** 2 创建创建数组的rdd 数据集*/
    val value: RDD[Range.Inclusive] = sc.parallelize(Array(1 to 10))

    /** 算子：  转换算子   行动算子   控制算子 */
    //val valueString: RDD[String] = value.map(s=>s+("123"))

    value.foreach(s=>println(s))


    /** list*/
    val valueTest = sc.makeRDD(List(1,2,3))
    val valuess = valueTest.map { x => x * x}
    println(valuess.collect().mkString(","))
    val rdd01s = sc.makeRDD(List(1,2,3,4,5,6))
    val r01s = rdd01s.map { x => x * x }
    println(r01s.collect().mkString(","))


    /** array */
    val rdd02s = sc.makeRDD(Array(1,2,3,4,5,6))
    val r02 = rdd02s.filter { x => x < 5}
    println(r02.collect().mkString(","))



    val rdd03 = sc.parallelize(List(1,2,3,4,5,6), 1)
    val r03 = rdd03.map { x => x + 1 }
    println(r03.collect().mkString(","))

    /* Array */
    val rdd04 = sc.parallelize(List(1,2,3,4,5,6), 1)
    val r04 = rdd04.filter { x => x > 3 }
    println(r04.collect().mkString(","))


    /**
      * 大家看到了RDD本质就是一个数组，因此构造数据时候使用的是List（链表）和Array（数组）类型。
      * 　　第二类方式是通过文件系统构造RDD，代码如下所示：
      * */
    val rdd:RDD[String] = sc.textFile("file:///D:/sparkdata.txt", 1)
    val r:RDD[String] = rdd.flatMap { x => x.split(",") }
    println(r.collect().mkString(","))



    /**
      * 构造了RDD对象了，接下来就是如何操作RDD对象了，RDD的操作分为转化操作（transformation）和行动操作（action），
      * RDD之所以将操作分成这两类这是和RDD惰性运算有关，当RDD执行转化操作时候，实际计算并没有被执行，只有当RDD执行行动
      * 操作时候才会促发计算任务提交，执行相应的计算操作。区别转化操作和行动操作也非常简单，转化操作就是从一个RDD产生
      * 一个新的RDD操作，而行动操作就是进行实际的计算。
      *
      * 　　下面是RDD的基础操作API介绍
      * */

    /** 操作类型

    函数名

    作用

    转化操作

    map()

    参数是函数，函数应用于RDD每一个元素，返回值是新的RDD

    flatMap()

    参数是函数，函数应用于RDD每一个元素，将元素数据进行拆分，变成迭代器，返回值是新的RDD

    filter()

    参数是函数，函数会过滤掉不符合条件的元素，返回值是新的RDD

    distinct()

    没有参数，将RDD里的元素进行去重操作

    union()

    参数是RDD，生成包含两个RDD所有元素的新RDD

    intersection()

    参数是RDD，求出两个RDD的共同元素

    subtract()

    参数是RDD，将原RDD里和参数RDD里相同的元素去掉

    cartesian()

    参数是RDD，求两个RDD的笛卡儿积

    行动操作

    collect()

    返回RDD所有元素

    count()

    RDD里元素个数

    countByValue()

    各元素在RDD中出现次数

    reduce()

    并行整合所有RDD数据，例如求和操作

    fold(0)(func)

    和reduce功能一样，不过fold带有初始值

    aggregate(0)(seqOp,combop)

    和reduce功能一样，但是返回的RDD数据类型和原RDD不一样

    foreach(func)

    对RDD每个元素都是使用特定函数*/




    /** 转化算子 */
    /** * ***********************************************************************************************************************************/
    val rddInt:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,2,5,1))
    val rddStr:RDD[String] = sc.parallelize(Array("a","b","c","d","b","a"), 1)
    val rddFile:RDD[String] = sc.textFile("file:///D:/sparkdata.txt", 1)

    val rdd01:RDD[Int] = sc.makeRDD(List(1,3,5,3))
    val rdd02:RDD[Int] = sc.makeRDD(List(2,4,5,1))

    /* map操作 */
    println("======map操作======")
    println(rddInt.map(x => x + 1).collect().mkString(","))
    println("======map操作======")
    /* filter操作 */
    println("======filter操作======")
    println(rddInt.filter(x => x > 4).collect().mkString(","))
    println("======filter操作======")
    /* flatMap操作 */
    println("======flatMap操作======")
    println(rddFile.flatMap { x => x.split(",") }.first())
    println("======flatMap操作======")
    /* distinct去重操作 */
    println("======distinct去重======")
    println(rddInt.distinct().collect().mkString(","))
    println(rddStr.distinct().collect().mkString(","))
    println("======distinct去重======")
    /* union操作 */
    println("======union操作======")
    println(rdd01.union(rdd02).collect().mkString(","))
    println("======union操作======")
    /* intersection操作 */
    println("======intersection操作======")
    println(rdd01.intersection(rdd02).collect().mkString(","))
    println("======intersection操作======")
    /* subtract操作 */
    println("======subtract操作======")
    println(rdd01.subtract(rdd02).collect().mkString(","))
    println("======subtract操作======")
    /* cartesian操作 */
    println("======cartesian操作======")
    println(rdd01.cartesian(rdd02).collect().mkString(","))
    println("======cartesian操作======")
    /** * ***********************************************************************************************************************************/


    /** 行动操作代码如下：*/
    /** * ***********************************************************************************************************************************/
    val rddInts:RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,2,5,1))
    val rddStrs:RDD[String] = sc.parallelize(Array("a","b","c","d","b","a"), 1)

    /* count操作 */
    println("======count操作======")
    println(rddInts.count())
    println("======count操作======")
    /* countByValue操作 */
    println("======countByValue操作======")
    println(rddInts.countByValue())
    println("======countByValue操作======")
    /* reduce操作 */
    println("======countByValue操作======")
    println(rddInts.reduce((x ,y) => x + y))
    println("======countByValue操作======")
    /* fold操作 */
    println("======fold操作======")
    println(rddInts.fold(0)((x ,y) => x + y))
    println("======fold操作======")
    /* aggregate操作 */
    println("======aggregate操作======")
    val res:(Int,Int) = rddInts.aggregate((0,0))((x,y) => (x._1 + x._2,y),(x,y) => (x._1 + x._2,y._1 + y._2))
    println(res._1 + "," + res._2)
    println("======aggregate操作======")
    /* foeach操作 */
    println("======foeach操作======")
    println(rddStrs.foreach { x => println(x) })
    println("======foeach操作======")
    /** * ***********************************************************************************************************************************/














    sc.stop()
  }

}

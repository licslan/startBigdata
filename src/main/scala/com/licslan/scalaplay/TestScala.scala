package com.licslan.scalaplay

object TestScala {

  def main(args: Array[String]): Unit = {
    val x = 10
    if(x<10){
      println(x)
    }else{
      println(x/2)
    }

    val y = 30
    if (y < 20) println("x 小于 20")
    else println("x 大于 20")


    val z = 30
    if (z == 10) println("X 的值为 10")
    else if (z == 20) println("X 的值为 20")
    else if (z == 30) println("X 的值为 30")
    else println("无法判断 X 的值")



    /** 数组*/
    val xy = new Array[String](3)
    val xyz =  Array("11","hh")
    println(xy(1))
    println(xyz(1))


    var myList = Array(1.9, 2.9, 3.4, 3.5)

    // 输出所有数组元素
    for ( x <- myList ) {
      println( x )
    }

    // 计算数组所有元素的总和
    var total = 0.0;
    for ( i <- 0 to (myList.length - 1)) {
      total += myList(i);
    }
    println("总和为 " + total);

    // 查找数组中的最大元素
    var max = myList(0);
    for ( i <- 1 to (myList.length - 1) ) {
      if (myList(i) > max) max = myList(i);
    }
    println("最大值为 " + max);

  }
}

package com.drunk2013.spark.rdd

import org.apache.spark.{SharedSparkContext, SharedSparkSession, SparkContext}

/**
  * Created by shuangfu on 17-2-7.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
object TreeAggregate extends SharedSparkSession {
  def seq(a: Int, b: Int): Int = {
    println("seq=a:" + a + ",b:" + b)
    math.min(a, b)
  }

  def comb(a: Int, b: Int): Int = {
    println("comb=a:" + a + ",b:" + b)
    a + b
  }


  def main(args: Array[String]): Unit = {
    val data = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5, 8, 9))
    data.aggregate(3)(seq, comb)
    data.treeAggregate(3)(seq, comb)
  }
}

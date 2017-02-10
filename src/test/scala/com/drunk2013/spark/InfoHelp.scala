package com.drunk2013.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Created by shuangfu on 17-2-7.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
object InfoHelp {
  def show(name: String, obj: Any): Unit = {
    println(s"\n=======================${name}============================")
    println("1.数据类型:" + obj.getClass)
    println("2.数据样例:")
    obj match {
      case df: DataFrame => {
        df.printSchema()
        df.take(100).foreach(println(_))
      }
      case rdd: RDD[Any] => {
        println("partirion size:" + rdd.getNumPartitions)
        rdd.collect().take(100).foreach(println(_))
      }
      case array: Array[AnyVal] => {
        array.foreach(println(_))
      }
      case vector: Vector => {
        vector.toArray.foreach(println(_))
      }

      case _ => println(obj)
    }
    println(s"-------------------------end-------------------------------\n")
  }

}

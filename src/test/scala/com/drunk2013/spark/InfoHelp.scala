package com.drunk2013.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by shuangfu on 17-2-7.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
object InfoHelp {
  def show(name: String, obj: Any): Unit = {
    println(s"\n=======================${name}============================")
    println("1.数据类型:" + obj.getClass)
    println("2.数据结构:")
    obj match {
      case df: DataFrame => {
        df.printSchema()
        println("3.数据样例:")
        df.take(50).foreach(println(_))
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

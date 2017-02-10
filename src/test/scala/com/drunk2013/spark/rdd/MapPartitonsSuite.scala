package com.drunk2013.spark.rdd

import com.drunk2013.spark.InfoHelp
import org.apache.spark.rdd.RDD
import org.apache.spark.{SharedSparkContext, SparkFunSuite}

import scala.collection.mutable

/**
  * Created by shuangfu on 17-2-10.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
class MapPartitonsSuite extends SparkFunSuite with SharedSparkContext with Serializable {

  def genData[S <: (String, String, Int)](s: S): Array[(String, Int)] = {
    Array((s._1, s._3))
  }

  test("测试mapPartition和flatMap的功能") {
    println("开始测试mapPartition和flatMap的功能")
    val rdd = sc.parallelize(
      List(
        ("zsf", "maths", 90),
        ("jy", "maths", 80),
        ("wzm", "maths", 70),
        ("zsf", "english", 70),
        ("jy", "english", 90),
        ("wzm", "english", 80),
        ("zsf", "compute", 80),
        ("jy", "compute", 90),
        ("wzm", "compute", 70)
      ), 2)

    InfoHelp.show("rdd", rdd)
    val rddFlat = rdd.flatMap(x => Array(List(x._1, x._3)))
    InfoHelp.show("rdd-flatMap", rddFlat)
    //val scoreRDD = rdd.mapPartitions { scoresIter =>
    //  scoresIter.flatMap(x => Array((x._1, x._3)))
    //}

    val scoreRDD = rdd.mapPartitionsWithIndex { (index, rowIter) =>
      rowIter.map(x => (index, x._1, x._2, x._3))
    }
    InfoHelp.show("scoreRDD", scoreRDD)

    val resultRDD = scoreRDD.map(x => (x._2, x._4))
      .reduceByKey(_ + _)

    InfoHelp.show("resultRD", resultRDD)

    val expected = mutable.HashMap("zsf" -> 240, "wzm" -> 220, "jy" -> 260)

    assert(expected == resultRDD.collect().toMap)

  }


}
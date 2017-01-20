package org.github2013.spark.ml

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by shuangfu on 17-1-20.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_ml_src")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file = sc.textFile("file:/opt/spark/data/mllib/sample_fpgrowth.txt")
    file.collect().foreach(println)

  }

}

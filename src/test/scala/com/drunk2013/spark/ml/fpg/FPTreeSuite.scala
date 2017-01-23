package com.drunk2013.spark.ml.fpg.fpg

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

/**
  * Created by shuangfu on 17-1-23.
  */
class FPTreeSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("添加 transaction item"){
    val tree = new FPTree[String]
  }

}

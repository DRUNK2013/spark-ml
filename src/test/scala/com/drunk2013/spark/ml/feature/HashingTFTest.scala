package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.InfoHelp
import com.drunk2013.spark.util.Utils
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by shuangfu on 17-2-6.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
class HashingTFTest extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new HashingTF)
  }

  test("HashingTF测试...") {
    val df = Seq((0, "a a b b c d".split(" ").toSeq)).toDF("id", "words")
    InfoHelp.show("df", df)
    val n = 100
    //val hashingTF = new org.apache.spark.ml.feature.HashingTF()
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(n)
    InfoHelp.show("hashingTF", hashingTF)

    val output = hashingTF.transform(df)
    InfoHelp.show("output", output)

    val attrGroup = AttributeGroup.fromStructField(output.schema("features"))
    InfoHelp.show("attrGroup", attrGroup)

    require(attrGroup.numAttributes == Some(n))
    val features = output.select("features").first().getAs[Vector](0)
    InfoHelp.show("features", features)

    // Assume perfect hash on "a", "b", "c", and "d".
    //返回的结果应该有四个词,a,b,c,d
    def idx: Any => Int = murmur3FeatureIdx(n)

    val expected = Vectors.sparse(n,
      Seq((idx("a"), 2.0), (idx("b"), 2.0), (idx("c"), 1.0), (idx("d"), 1.0)))
    InfoHelp.show("expected", expected)
    assert(features == expected)
  }

  test("测试HashingTF binary模式下的词频") {
    val df = Seq((0, "a a b c c c".split("\\s+").toSeq)).toDF("id", "words")
    InfoHelp.show("df", df)
    val n = 100
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("features")
      .setNumFeatures(n)
      .setBinary(true)

    InfoHelp.show("hashingTF", hashingTF)

    val output = hashingTF.transform(df)
    InfoHelp.show("output", output)

    val features = output.select("features").first().getAs[Vector](0)
    InfoHelp.show("features", features)

    def idx: Any => Int = murmur3FeatureIdx(n)

    val expected = Vectors.sparse(n,
      Seq((idx("a"), 1.0), (idx("b"), 1.0), (idx("c"), 1.0))
    )
    assert(features == expected)
  }

  private def murmur3FeatureIdx(numFeatures: Int)(term: Any): Int = {
    Utils.nonNegativeMod(HashingTFLIB.murmur3Hash(term), numFeatures)
  }
}

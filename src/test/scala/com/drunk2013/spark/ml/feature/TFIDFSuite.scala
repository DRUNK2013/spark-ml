package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.InfoHelp
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext

/**
  * Created by shuangfu on 17-1-20.
  */

class TFIDFSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("功能测试") {

    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    InfoHelp.show("sentenceData", sentenceData)

    //对句子进行分词处,把sentence转换成words
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    InfoHelp.show("tokenizer", tokenizer)

    val wordsData = tokenizer.transform(sentenceData)
    InfoHelp.show("wordsData", wordsData)


    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(2000)

    val featurizedData = hashingTF.transform(wordsData)
    InfoHelp.show("featurizedData", featurizedData)
    // alternatively, Count Vectorizer can also be used to get term frequency vectors

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    InfoHelp.show("rescaledData", rescaledData)

  }

}

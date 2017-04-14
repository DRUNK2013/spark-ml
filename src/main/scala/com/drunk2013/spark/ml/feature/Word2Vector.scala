package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.util.{HasMaxIter, HasSeed, HasStepSize}
import org.apache.spark.ml.Model
import org.apache.spark.ml.linalg.{BLAS2, Vectors}
import org.apache.spark.ml.linalg.{BLAS, Vectors}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util.{Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by shuangfu on 17-2-9.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
/**
  * word2vec 特征提取
  */
private[feature] trait Word2VectorBase extends Params with HasInputCol with HasOutputCol
  with HasMaxIter with HasStepSize with HasSeed {

  /**
    * 设置向量的大小,即词的向量规模
    * The dimension of the code that you want to transform from words.
    * Default: 100
    *
    * @group param
    */
  final val vectorSize = new IntParam(this, "vectorSize",
    "the dimension of codes after transforming form word(>0)",
    ParamValidators.gt(0))
  setDefault(vectorSize -> 100)

  def getVectorSize: Int = $(vectorSize)

  /** 一个词,前后取相关计算对词个数
    * The window size (context words from [-window, window]).
    * Default: 5
    *
    * @group expertParam
    */
  final val windowSize = new IntParam(this, "windowSize",
    "the windos size ,(context words form [-windowSize,windowSize])(>0)",
    ParamValidators.gt(0))
  setDefault(windowSize -> 5)

  def getWindowSize: Int = $(windowSize)

  /**
    * 为所有的句子,进行设置分区大小
    * Number of partitions for sentences of words.
    * Default: 1
    *
    * @group param
    */
  final val numPartitions = new IntParam(this, "numPartitions",
    "number of partitions for sentences of words (>0)",
    ParamValidators.gt(0)
  )
  setDefault(numPartitions -> 1)

  def getNumPartitions: Int = $(numPartitions)

  /**
    * 词的数据集中出现对频次
    * The minimum number of times a token must appear to be included in the word2vec model's
    * vocabulary.
    * Default: 5
    *
    * @group param
    */
  final val minCount = new IntParam(this, "minCount",
    "the minimum number of times a taken must appear to be included in word2vec model's vocabulary (>=0)",
    ParamValidators.gt(0))
  setDefault(minCount -> 5)

  def getMinCount: Int = $(minCount)

  /**
    * 句子对最大长度,超过此长度,将进行切分
    * Sets the maximum length (in words) of each sentence in the input data.
    * Any sentence longer than this threshold will be divided into chunks of
    * up to `maxSentenceLength` size.
    * Default: 1000
    *
    * @group param
    */
  final val maxSentenceLength = new IntParam(this, "maxSentenceLength",
    "(in words)of each sentence in the input data. Any sentence longer than this throshold will " +
      "be divided into chunks up to the size(>0)",
    ParamValidators.gt(0))
  setDefault(maxSentenceLength -> 1000)

  def getMaxSentenceLength: Int = $(maxSentenceLength)

  setDefault(stepSize -> 0.025)
  setDefault(maxIter -> 1)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // val typeCandidates = List(new ArrayType(StringType, true), new ArrayType(StringType, false))
    // SchemaUtils.checkColumnTypes(schema, $(inputCol), typeCandidates)
    // SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
    null
  }
}

//final class Word2Vector private[ml](
//                                     override val uid: String,
//                                     private val word2VectorModelLIB)
//  extends Model[Word2VectorModel] with Word2VectorBase {
//
//  def this() = this(Identifiable.randomUID("w2v"))
//
//  def setInputCol(value: String): this.type = set(inputCol, value)
//
//  def setOutputCol(value: String): this.type = set(outputCol, value)
//
//  def setVectorSize(value: Int): this.type = set(vectorSize, value)
//
//  def setWindowSize(value: Int): this.type = set(windowSize, value)
//
//  def setStepSize(value: Int): this.type = set(stepSize, value)
//
//  def setNumPartitions(value: Int): this.type = set(numPartitions, value)
//
//  def setMaxIter(value: Int): this.type = set(maxIter, value)
//
//  def setSeed(value: Int): this.type = set(seed, value)
//
//  def setMinCount(value: Int): this.type = set(minCount, value)
//
//  def setMaxSentenceLength(value: Int): this.type = set(maxSentenceLength, value)
//
//
//}

class Word2VectorModel private[feature](
                                         override val uid: String,
                                         private val wordVectors: Word2VectorModelLIB
                                       )
  extends Model[Word2VectorModel] with Word2VectorBase {

  @transient lazy val getVectors: DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val wordVec = wordVectors.getVectors.mapValues(vec => Vectors.dense(vec.map(_.toDouble)))
    spark.createDataFrame(wordVec.toSeq).toDF("word", "vector")
  }

  def findSynonyms(word: String, num: Int): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    spark.createDataFrame(wordVectors.findSynonyms(word, num)).toDF("word", "similarity")
  }

  //  def findSynonyms(vector: Vector, num: Int): DataFrame = {
  //    val spark = SparkSession.builder().getOrCreate()
  //    spark.createDataFrame(wordVectors.findSynonyms(vectorL,num)).toDF()
  //
  //  }

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val vectors = wordVectors.getVectors
      .mapValues(vv => Vectors.dense(vv.map(_.toDouble)))
      .map(identity)
    val bVectors = dataset.sparkSession.sparkContext.broadcast(vectors)
    val d = $(vectorSize)

    val word2Vector = udf { sentence: Seq[String] =>
      if (sentence.isEmpty) {
        Vectors.sparse(d, Array.empty[Int], Array.empty[Double])
      } else {
        val sum = Vectors.zeros(d)
        sentence.foreach { word =>
          bVectors.value.get(word).foreach { v =>
            BLAS2.axpy(1.0, v, sum)
          }
        }

      }

    }
    null
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Word2VectorModel = {
    val copied = new Word2VectorModel(uid, wordVectors)
    copyValues(copied, extra).setParent(parent)
  }
}

object Word2VectorModel {

}

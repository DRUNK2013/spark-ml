package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.util.{HasMaxIter, HasSeed, HasStepSize}
import org.apache.spark.ml.param.{IntParam, ParamValidators, Params}


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

}

//final class Word2VectorModel private[ml](
//                                          override val uid:String,
//                                          private val word2VectorModelLIB )
//  extends Model[Word2VectorModel] with Word2VectorBase with MLWritable {
//}

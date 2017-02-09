package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.util.{Logging, Utils}
import org.apache.spark.mllib.util.Loader
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by shuangfu on 17-2-9.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
private case class VocabWord(
                              var word: String,
                              var cn: Int,
                              var point: Array[Int],
                              var code: Array[Int],
                              var codeLen: Int
                            )

class Word2VectorLIB extends Serializable with Logging {
  private var vectorSize = 100
  private var learningRate = 0.025
  private var numPartitions = 1
  private var numIterations = 1
  private var seed = Utils.random.nextLong()
  private var minCount = 5
  private var maxSentenceLength = 1000
  private var window = 5

  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6
  private val MAX_CODE_LENGTH = 40

  private var trainWordsCount = 0L
  private var vocabSize = 0

  @transient private var vocab: Array[VocabWord] = null
  @transient private var vocabHash = mutable.HashMap.empty[String, Int]

  private def learnVocab[S <: Iterable[String]](dataset: RDD[S]): Unit = {
    //词频平铺成集合
    val words = dataset.flatMap(x => x)
    //对每个词进行频次统计,并装入到VocabWord,返回数组
    vocab = words.map(w => (w, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > minCount)
      .map(x => VocabWord(
        x._1,
        x._2,
        new Array[Int](MAX_CODE_LENGTH),
        new Array[Int](MAX_CODE_LENGTH),
        0))
      .collect()
      .sortWith((a, b) => a.cn > b.cn)

    //词类(去重后的词量)大小
    vocabSize = vocab.length
    require(vocabSize > 0,
      s"词汇总量应该大于0,检查minCount参数是否设置过大,导致过滤掉大量对词汇.The vocabulary size should be > 0," +
        s"you may need to check the setting of minCount which could be large enough to remove all your words in sentence.")

    var a = 0
    while (a < vocabSize) {
      this.vocabHash += vocab(a).word -> a //为每个词,添加唯一hash值
      this.trainWordsCount += vocab(a).cn //统计训练词总量
      a += 1
    }

    logInfo(s"vocabSize=${vocabSize},trainWordsCount=${trainWordsCount}")
  }

  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  /**
    * 设置句子对最大长度
    * Sets the maximum length (in words) of each sentence in the input data.
    * Any sentence longer than this threshold will be divided into chunks of
    * up to `maxSentenceLength` size (default: 1000)
    */
  def setMaxSentenceLength(maxSentenceLength: Int): this.type = {
    require(maxSentenceLength > 0,
      s"句子的长度必须大于0,Maximum length of sentences must be positive but got ${maxSentenceLength}")
    this.maxSentenceLength = maxSentenceLength
    this
  }

  /**
    * Sets vector size (default: 100).
    * 设置向量对大小
    */
  def setVectorSize(vectorSize: Int): this.type = {
    require(vectorSize > 0,
      s"向量规模大小必须大于0.Vector size must be positive ,but got ${vectorSize}")
    this.vectorSize = vectorSize
    this
  }

  /**
    * Sets initial learning rate (default: 0.025).
    *
    * @param learningRate
    * @return
    */
  def setLeaningRate(learningRate: Double): this.type = {
    require(learningRate > 0,
      s"学习率必须大于0,learningRate must be positive,but got ${learningRate}")
    this.learningRate = learningRate
    this
  }

  /**
    * 设置分区大小
    *
    * @param numPartitions
    * @return
    */
  def setNumPartions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"number partitions must be positivef,bug got ${numPartitions}")
    this.numPartitions = numPartitions
    this
  }

  /**
    * 设置迭代次数
    *
    * @param numIterations
    * @return
    */
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations >= 0,
      s"number iterations must be nonnegative ,but got ${numIterations}")
    this.numIterations = numIterations
    this
  }

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /**
    * 设置词窗口前后偏移步长
    *
    * @param window
    * @return
    */
  def setWindowSize(window: Int): this.type = {
    require(window > 0,
      s"Window of words must be positive ,but got ${window}")
    this.window = window
    this
  }

  /**
    * 设置最小词频
    *
    * @param minCount
    * @return
    */
  def setMinCount(minCount: Int): this.type = {
    require(minCount >= 0,
      s"minimum number of times must be nonnegative,but got ${minCount}")
    this.minCount = minCount
    this
  }


}


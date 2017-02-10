package com.drunk2013.spark.ml.feature

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.drunk2013.spark.util.{BoundedPriorityQueue, Logging, Utils}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import sun.util.resources.cldr.to.CurrencyNames_to

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

  /**
    * 指数表编码,logic函数,目标是把很大或很小对数,降低精度,合并成一个数
    * 把数据等分成EXP_TABLE_SIZE份,X轴的数值范围:[-MAX_EXP,MAX_EXP]
    * precompute the exp() table
    * Easy to find that sigmoid(x) values almost keep unchanged when x is too large or too small.
    * Let’s assume sigma(x)=1 when x>MAX_EXP, and sigma(x)=0 when x<-MAX_EXP.
    * The code splits the range [-MAX_EXP, MAX_EXP] into EXP_TABLE_SIZE pieces.
    * In each piece, we assume the sigma(x) values are the same.
    *
    * @return
    */
  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      //(2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP  数值范围:[-MAX_EXP , MAX_EXP]
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  private def createBinaryTree(): Unit = {
    val count = new Array[Long](vocabSize * 2 + 1)
    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0
    while (a < vocabSize) {
      count(a) = vocab(a).cn
      a += 1
    }
    while (a < 2 * vocabSize) {
      count(a) = 1e9.toInt
      a += 1
    }

    var pos1 = vocabSize - 1
    var pos2 = vocabSize

    var min1i = 0
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }

      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }

      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }

    //Now assign binary code to each vocabulary word
    var i = 0
    a = 0
    while (a < vocabSize) {
      var b = a
      i = 0
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(i)
        point(i) = b
        i += 1
        b = parentNode(b)
      }
      vocab(a).codeLen = i
      vocab(a).point(0) = vocabSize - 2
      b = 0
      while (b < i) {
        vocab(a).code(i - b - 1) = code(b)
        vocab(a).code(i - b) = point(a) - vocabSize
        b += 1
      }
      a += 1
    }


  }

  def fit[S <: Iterable[String]](dataset: RDD[S]): Word2VectorModelLIB = {

    learnVocab(dataset)
    createBinaryTree()

    val sc = dataset.context
    val expTable = sc.broadcast(createExpTable())
    val bcVocab = sc.broadcast(vocab)
    val bcVocabHash = sc.broadcast(vocabHash)

    //each partition is a collection of sentences
    //will be translated int arrays of index interge
    val sentences: RDD[Array[Int]] = dataset.mapPartitions { sentenceIter =>
      //each sentence will map to 0 or more Array[Int]
      sentenceIter.flatMap { sentence =>
        val wordIndexes = sentence.flatMap(bcVocabHash.value.get)
        wordIndexes.grouped(maxSentenceLength).map(_.toArray)
      }
    }
    // each partition is a collection of sentences,
    // will be translated into arrays of Index integer
    val sentences2: RDD[Array[Int]] = dataset.mapPartitions { sentenceIter =>
      // Each sentence will map to 0 or more Array[Int]
      sentenceIter.flatMap { sentence =>
        // Sentence of words, some of which map to a word index
        val wordIndexes = sentence.flatMap(s => bcVocabHash.value.get(s))
        // break wordIndexes into trunks of maxSentenceLength when has more
        wordIndexes.grouped(maxSentenceLength).map(_.toArray)
      }
    }
    return null
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


/**
  * Word2vec模型
  *
  * @param wordIndex   maps each word to an index,which can retrieve the corresponing vector from wordVectors
  * @param wordVectors array of length numWords * vectorSize , vector corresponing to the word mapped with
  *                    index 1 can be retrieved by the slice(i * vectorSize ,i * vectorSize + vectorSize)
  *
  */
class Word2VectorModelLIB private[feature](
                                            val wordIndex: Map[String, Int],
                                            val wordVectors: Array[Float]
                                          ) extends Serializable {
  //词种类量
  private val numWords = wordIndex.size
  //vectorSize:dimension of each word's vector.
  //词的纬度大小
  private val vectorSize = wordVectors.length / numWords

  //wordList:Ordered list of words obtained form wordIndex.
  //对word的索引进行排序
  private val wordList: Array[String] = {
    val (wl, _) = wordIndex.toSeq.sortBy(_._2).unzip
    wl.toArray
  }

  // wordVecNorms: Array of length numWords, each value being the Euclidean norm
  //               of the wordVector.
  private val wordVecNorms: Array[Double] = {
    val wordVecNorms = new Array[Double](numWords)
    var i = 0
    while (i < numWords) {
      val vec = wordVectors.slice(i * vectorSize, i * vectorSize + vectorSize)
      wordVecNorms(i) = blas.snrm2(vectorSize, vec, 1)
      i += 1
    }
    wordVecNorms
  }

  def this(model: Map[String, Array[Float]]) = {
    this(Word2VectorModelLIB.buildWordIndex(model), Word2VectorModelLIB.buildWordVectors(model))
  }

  /**
    * 把word转化为向量
    *
    * @param word
    * @return
    */
  def trainform(word: String): Vector = {
    wordIndex.get(word) match {
      case Some(ind) =>
        val vec = wordVectors.slice(ind * vectorSize, ind * vectorSize + vectorSize)
        Vectors.dense(vec.map(_.toDouble))
      case None =>
        throw new IllegalStateException(s"$word not in vocabulary")
    }
  }

  /**
    * 根据输入对word向量,找出最相近对num个近义词,并过滤掉给定的词,通过余玄夹角值计算
    * Find synonym of the vector representation of word ,possibly include any word in the model vocabulary whose
    * vector representation is the supplied vector
    *
    * @param vector  vector representation of a word
    * @param num     number of synonyms to find
    * @param wordOpt optionally , a word to reject from the result list
    * @return array of (word,cosineSimilarity)
    */
  private def findSynonyms(
                            vector: Vector,
                            num: Int,
                            wordOpt: Option[String]
                          ): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")

    val fVector = vector.toArray.map(_.toFloat)
    val cosineVec = Array.fill[Float](numWords)(0)
    val alpha: Float = 1
    val beta: Float = 1

    //Normalize input vector before blas.sgemv to avoid Inf value
    val vecNorm = blas.snrm2(vectorSize, fVector, 1)
    if (vecNorm != 0.0f) {
      blas.sscal(vectorSize, 1 / vecNorm, fVector, 0, 1)
    }

    blas.sgemv("T", vectorSize, numWords, alpha, wordVectors, vectorSize, fVector, 1, beta, cosineVec, 1)

    val cosVec = cosineVec.map(_.toDouble)
    var ind = 0
    while (ind < numWords) {
      val norm = wordVectors(ind)
      if (norm == 0.0) {
        cosVec(ind) = 0.0
      } else {
        cosVec(ind) /= norm
      }
      ind += 1
    }

    val pq = new BoundedPriorityQueue[(String, Double)](num + 1)(Ordering.by(_._2))

    for (i <- cosVec.indices) {
      pq += Tuple2(wordList(i), cosVec(i))
    }

    val scored = pq.toSeq.sortBy(-_._2)
    val filtered = wordOpt match {
      case Some(w) => scored.filter(tup => w != tup._1)
    }

    filtered.take(num).toArray
  }

  /** *
    * find synonym of a word ; do not include the word itself in results
    *
    * @param word a word
    * @param num  number of synonyms to find
    * @return array of (word , cosineSimilarity)
    */
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val vector = trainform(word)
    findSynonyms(vector, num, Some(word))
  }

  /**
    * find synonym of the vector representation of a word ,possibly including any words in the model vocabulary
    * whose vector representation is the supplied vector
    *
    * @param vector vector representation of a word
    * @param num    number of synonyms to find
    * @return array of (word,cosineSimilarity)
    */
  def findSynonyms(vector: Vector, num: Int): Array[(String, Double)] = {
    findSynonyms(vector, num, None)
  }

  def getVectors: Map[String, Array[Float]] = {
    wordIndex.map { case (word, ind) =>
      (word, wordVectors.slice(vectorSize * ind, vectorSize * ind + vectorSize))
    }
  }
}


/**
  * word2VectorModelLIB 半生对象,提供辅助工具
  */
object Word2VectorModelLIB {
  /**
    * 提取模型中对,word并添加索引
    *
    * @param model
    * @return
    */
  private def buildWordIndex(model: Map[String, Array[Float]]): Map[String, Int] = {
    model.keys.zipWithIndex.toMap
  }

  /**
    * 构建所有对word的向量,并按照word的索引顺序进行合并成数组
    *
    * @param model
    * @return
    */
  private def buildWordVectors(model: Map[String, Array[Float]]): Array[Float] = {
    require(model.nonEmpty, "Word2vector map should bo non-empty!!")
    val (vectorSize, numWords) = (model.head._2.length, model.size)
    val wordList = model.keys.toArray
    val wordVectors = new Array[Float](vectorSize * numWords)
    var i = 0
    while (i < numWords) {
      Array.copy(model(wordList(i)), 0, wordVectors, i * vectorSize, vectorSize)
      i += 1
    }
    wordVectors
  }

}
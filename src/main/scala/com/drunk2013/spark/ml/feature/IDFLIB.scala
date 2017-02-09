package com.drunk2013.spark.ml.feature

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}

/**
  * Created by shuangfu on 17-2-7.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
/**
  * IDF mllib类,通过RDD实现
  * ​ 此处 |D||D| 是语料库中总的文档数。公式中使用log函数，当词出现在所有文档中时，它的IDF值变为0。加1是为了避免分母为0的情况。TF-IDF 度量值表示如下：
  * 词语由t表示，文档由d表示，语料库由D表示。词频TF(t,d)是词语t在文档d中出现的次数。文件频率DF(t,D)是包含词语的文档的个数。
  * 如果我们只使用词频来衡量重要性，很容易过度强调在文档中经常出现，却没有太多实际信息的词语，比如“a”，“the”以及“of”。
  * 如果一个词语经常出现在语料库中，意味着它并不能很好的对文档进行区分。TF-IDF就是在数值化文档信息，衡量词语能提供多少信息以区分文档。其定义如下：
  * TFIDF(t,d,D)=TF(t,d)⋅IDF(t,D)
  * IDF=(|D|+1)/(DF(t,d)+1),然后去log
  * IDF的值反应词的重要性,比如:
  * 若有m个文档,词"的"在每个文档都存在.则(|D|+1)/(DF(t,d)+1)为1,取log后为0
  *
  * @param minDocFreq
  */
class IDFLIB(val minDocFreq: Int) {
  def this() = this(0)

  /**
    * 对给定的RDD输入数据集,计算其IDFModel模型对象
    * 计算IDF向量
    * 逐层聚合
    *
    * @param dataset
    * @return
    */
  def fit(dataset: RDD[Vector]): IDFModelLIB = {
    val idf = dataset.treeAggregate(
      new IDFLIB.DocumentFrequencyAggregator(minDocFreq = minDocFreq))(
      seqOp = (df, v) => df.add(v),
      combOp = (df1, df2) => df1.merge(df2)
    ).idf()
    new IDFModelLIB(idf)
  }

}

private object IDFLIB {

  /** Document frequency aggregator. */
  /**
    * 计算文档的频次
    *
    * @param minDocFreq
    */
  class DocumentFrequencyAggregator(val minDocFreq: Int) extends Serializable {
    //number of document 文档(一行数据) 的数量
    private var m = 0L

    //document frequncy vector 文档频次向量,采用稀疏向量存储
    //即公式中:DF(t,D).文件频率DF(t,D)是包含词语的文档的个数.词语t,
    private var df: BDV[Long] = _

    def this() = this(0)

    private def isEmpty: Boolean = (0L == m)

    /**
      * Adds a new document.
      * 添加新对文档(句子),采用稀疏向量 SparseVector或DenseVector存储
      *
      * @param doc
      * @return
      */
    def add(doc: Vector): this.type = {
      if (isEmpty) {
        df = BDV.zeros(doc.size)
      }

      doc match {
        case SparseVector(size, indices, values) => {
          //如果是稀疏向量,进行解析.并存储到breeze模式的稀疏向量,以便计算idf向量
          val nnz = indices.length
          var k = 0
          while (k < nnz) {
            if (values(k) > 0) {
              df(indices(k)) += 1L //统计词频,为每个词数量进行累加
            }
            k += 1
          }
        }
        case DenseVector(values) => {
          //DenseVector模式数据解析
          val n = values.length
          var j = 0
          while (j < n) {
            if (values(j) > 0.0) {
              df(j) += 1L //统计词频,为每个词数量进行累加
            }
            j += 1
          }
        }
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense verctors are supported ,but got ${other.getClass}"
          )
      }
      m += 1
      this
    }


    //合并
    def merge(other: DocumentFrequencyAggregator): this.type = {
      if (!other.isEmpty) {
        m += other.m
        if (null == df) {
          df = other.df.copy
        } else {
          df += other.df
        }
      }
      this
    }

    /**
      * 计算idf,并返回成Vector
      * idf为逆向文档词频,是词对重要性指标,和词在出现对文档数成反比.
      * 所有对文档数/某词出现对文档数.然后去log
      *
      * @return
      */
    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document") //文档为空
      }
      val n = df.length
      val inv = new Array[Double](n)
      var j = 0
      while (j < n) {
        /*
       * If the term is not present in the minimum
       * number of documents, set IDF to 0. This
       * will cause multiplication in IDFModel to
       * set TF-IDF to 0.
       *
       * Since arrays are initialized to 0 by default,
       * we just omit changing those entries.
       * 数组大多数词频为0,因此为了方便计算,把分子和分母都加1
       */
        if (df(j) >= minDocFreq) {
          inv(j) = math.log((m + 1.0) / (df(j) + 1.0)) //计算IDF值
        }
        j += 1
      }
      Vectors.dense(inv)
    }
  }

}

/**
  * Represents an IDF model that can transform term frequency vectors.
  *
  * @param idf 计算完的idf向量
  */
class IDFModelLIB private[ml](val idf: Vector) extends Serializable {

  /**
    * dataset数据集接口,并进行分区计算
    *
    * @param dataset
    * @return
    */
  def transform(dataset: RDD[Vector]): RDD[Vector] = {
    //广播idf数据,idf为逆向文档词频,是词对重要性指标,和词在出现对文档数成反比.即某词的文档
    val bcIdf = dataset.context.broadcast(idf)
    dataset.mapPartitions(iter => iter.map(v => IDFModelLIB.transform(bcIdf.value, v)))
  }

  def transform(vector: Vector): Vector = IDFModelLIB.transform(idf, vector)


}

private object IDFModelLIB {

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector with a IDF vector
    * 计算TF-IDF,公式如下:
    * TF-IDF=TF(t,d) * IDF(t,D)
    * TF-IDF=HashingTF * IDF
    *
    * @param idf an IDF vector
    * @param v   a term frequency vector,即HasingTF统计对词频
    * @return 返回TF-IDF vector
    */
  def transform(idf: Vector, v: Vector): Vector = {
    val n = v.size
    v match {
      case SparseVector(size, indices, values) => {
        val nnz = indices.length
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {
          newValues(k) = values(k) * idf(indices(k)) //词频 * IDF
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      }
      case DenseVector(values) => {
        val newValues = new Array[Double](n)
        var j = 0
        while (j < n) {
          newValues(j) = values(j) * idf(j) //词频 * IDF
          j += 1
        }
        Vectors.dense(newValues)
      }
      case other => {
        throw new UnsupportedOperationException(
          //给定的向量数据类型不支持,只支持,稀疏向量和宽向量
          s"Oln;y sparse and dense vectors are supported but got ${other.getClass}"
        )
      }
    }
  }
}

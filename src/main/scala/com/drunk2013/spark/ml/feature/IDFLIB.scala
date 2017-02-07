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
  *
  * @param minDocFreq
  */
class IDFLIB(val minDocFreq: Int) {
  def this() = this(0)


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

    //document frequncy vector 文档频次向量
    private var df: BDV[Long] = _

    def this() = this(0)

    private def isEmpty: Boolean = (0L == m)

    /**
      * Adds a new document.
      * 添加新对文档(句子)
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
          val nnz = indices.length
          var k = 0
          while (k < nnz) {
            if (values(k) > 0) {
              df(indices(k)) += 1L
            }
            k += 1
          }
        }
        case DenseVector(values) => {
          val n = values.length
          var j = 0
          while (j < n) {
            if (values(j) > 0.0) {
              df(j) += 1L
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

    def idf(): Vector = {
      if (isEmpty) {
        throw new IllegalStateException("Haven't seen any document") //文档为空
      }
      val n = df.length
      val inv = new Array[Double](n)
      var j = 0
      while (j < n) {
        if (df(j) >= minDocFreq) {
          inv(j) = math.log((m + 1.0) / df(j) + 1.0)
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
class IDFModel private[ml](val idf: Vector) extends Serializable {

  /**
    * dataset数据集接口,并进行分区计算
    *
    * @param dataset
    * @return
    */
  def transform(dataset: RDD[Vector]): RDD[Vector] = {
    val bcIdf = dataset.context.broadcast(idf)
    dataset.mapPartitions(iter => iter.map(v => IDFModel.transform(bcIdf.value, v)))
  }

  def transform(vector: Vector): Vector = IDFModel.transform(idf, vector)


}

private object IDFModel {
  def transform(idf: Vector, v: Vector): Vector = {
    val n = v.size
    v match {
      case SparseVector(size, indices, values) => {
        val nnz = indices.length
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {
          newValues(k) = values(k) * idf(indices(k))
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      }
      case DenseVector(values) => {
        val newValues = new Array[Double](n)
        var j = 0
        while (j < n) {
          newValues(j) = values(j) * idf(j)
          j += 1
        }
        Vectors.dense(newValues)
      }
      case other => {
        throw new UnsupportedOperationException(
          s"Oln;y sparse and dense vectors are supported but got ${other.getClass}"
        )
      }
    }
  }
}

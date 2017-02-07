package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.util.Utils
import java.lang.{Iterable => JavaIterable}

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.SparkException
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.unsafe.hash.Murmur3_x86_32._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by shuangfu on 17-2-6.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
/*
 * mllib类库中的HashingTF,实现对词频(TF),IDF计算
  *
  * @param numFeatures number of features (default: 2^20^)
  */
class HashingTFLIB(val numFeatures: Int) extends Serializable {

  import HashingTFLIB._

  private var binary = false
  private var hashAlgorithm = HashingTFLIB.Murmur3

  def this() = this(1 << 20)

  /**
    * If true, term frequency vector will be binary such that non-zero term counts will be set to 1
    * (default: false)
    */
  def setBinary(value: Boolean): this.type = {
    binary = value
    this
  }

  def setHashAlgorithm(value: String): this.type = {
    hashAlgorithm = value
    this
  }

  def indexOf(term: Any): Int = {
    Utils.nonNegativeMod(getHashFunction(term), numFeatures)
  }

  /**
    * Get the hash function corresponding to the current [[hashAlgorithm]] setting.
    */
  private def getHashFunction: Any => Int = hashAlgorithm match {
    case Murmur3 => murmur3Hash
    case _ =>
      // This should never happen.
      throw new IllegalArgumentException(
        s"HashingTF does not recognize hash algorithm $hashAlgorithm")
  }

  /**
    * Transforms the input document into a sparse term frequency vector.
    * 对每个文档进行,设置词hash,并统计每个词的词频
    * 最后转换为词对稀疏矩阵
    *
    * @param document
    * @return
    */
  def transform(document: Iterable[_]): Vector = {
    val termFrequencies = mutable.HashMap.empty[Int, Double]
    val setTF = if (binary) (i: Int) => 1.0 else (i: Int) => termFrequencies.getOrElse(i, 0.0) + 1.0
    //设置词频
    val hashFunc: Any => Int = getHashFunction
    document.foreach { term =>
      val i = Utils.nonNegativeMod(hashFunc(term), numFeatures) //获取词对hash值
      termFrequencies.put(i, setTF(i))
    }
    Vectors.sparse(numFeatures, termFrequencies.toSeq)
  }

  /**
    * Transforms the input document into a sparse term frequency vector (Java version).
    */
  def transform(document: JavaIterable[_]): Vector = {
    transform(document.asScala)
  }

  /**
    * Transforms the input document to term frequency vectors.
    */
  def transform[D <: Iterable[_]](dataset: RDD[D]): RDD[Vector] = {
    dataset.map(this.transform)
  }

  /**
    * Transforms the input document to term frequency vectors (Java version).
    */
  def transform[D <: JavaIterable[_]](dataset: JavaRDD[D]): JavaRDD[Vector] = {
    dataset.rdd.map(this.transform).toJavaRDD()
  }


}

object HashingTFLIB {
  private[HashingTFLIB] val Native: String = "native"
  private[HashingTFLIB] val Murmur3: String = "murmur3"
  private val seed = 42

  /**
    * Calculate a hash code value for the term object using the native Scala implementation.
    * This is the default hash algorithm used in Spark 1.6 and earlier.
    */
  //  private[HashingTF] def nativeHash(term: Any): Int = term.##

  /**
    * Calculate a hash code value for the term object using
    * Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32).
    * This is the default hash algorithm used from Spark 2.0 onwards.
    * 哈希算法获取值
    */
  private[spark] def murmur3Hash(term: Any): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }
}

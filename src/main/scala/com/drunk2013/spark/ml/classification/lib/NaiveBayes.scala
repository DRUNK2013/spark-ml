package com.drunk2013.spark.ml.classification.lib

import com.drunk2013.spark.util.Logging
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.Loader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by shuangfu on 17-2-28.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
class NaiveBayesModel(
                       val labels: Array[Double],
                       val pi: Array[Double],
                       val theta: Array[Array[Double]],
                       val modelType: String
                     ) extends ClassificationModel with Serializable {
  override def predict(testData: RDD[Vector]): RDD[Double] = ???

  override def predict(testData: Vector): Double = ???
}

class NaiveBayes private(
                          private var lambda: Double,
                          private var modelType: String
                        ) extends Serializable with Logging {
  def this(lambda: Double) = this(lambda, NaiveBayes.Multinomial)

  def this() = this(1.0, NaiveBayes.Multinomial)

  def setLambda(lambda: Double): NaiveBayes = {
    require(lambda >= 0, s"Smoothing parameter must be nonnegative but got $lambda")
    this.lambda = lambda
    this
  }

  def getLambda: Double = lambda

  def setModelType(modelType: String): NaiveBayes = {
    require(NaiveBayes.supportedModelTypes.contains(modelType),
      s"NaiveBayes was created with an unknown modelType:$modelType")
    this.modelType = modelType
    this
  }

  def getModelType: String = this.modelType

  //
  //  def run(data: RDD[LabeledPoint]): NaiveBayesModel = {
  //    val spark = SparkSession
  //      .builder()
  //      //.sparkContext(data.context)
  //      .getOrCreate()
  //
  //    import spark.implicits._
  //    val nb = new NaiveBayes()
  //        .setModelType(modelType)
  //      .setS
  //  }
}

object NaiveBayes {
  private val Multinomial: String = "multinomial"
  private val Bernoulli: String = "bernoulli"
  private val supportedModelTypes = Set(Multinomial, Bernoulli)

  //  def train(input: RDD[LabeledPoint]): NaiveBayesModel = {
  //
  //  }
}


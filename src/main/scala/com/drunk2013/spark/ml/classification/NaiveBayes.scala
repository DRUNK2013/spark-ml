package com.drunk2013.spark.ml.classification

import com.drunk2013.spark.util.{HasWeightCol, PredictorParams}
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg._
import com.drunk2013.spark.util.Matrix
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, ParamValidators}
import org.apache.spark.sql.Dataset

/**
  * Created by shuangfu on 17-2-14.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */

//private[classification] trait NaiveBayesParams extends PredictorParams with HasWeightCol {
//
//  /**
//    * the smoothing paramter.
//    * default 1.0
//    */
//  final val smoothing: DoubleParam = new DoubleParam(
//    this, "smoothing", "The smoothing parametor.", ParamValidators.gtEq(0))
//
//  final def getSmoothing: Double = $(smoothing)
//
//  /**
//    * 贝叶斯模型
//    * multinomial:多项分布
//    * bernoulli:伯努力分布,即二项式分布
//    */
//  final val modelType: Param[String] = new Param[String](this, "modelType", "The model type which is a " +
//    "string(case sensitive). Supported options:multinomial(default) and bernoulli")
//
//  final def getModelType: String = $(modelType)
//}
//
//class NaiveBayes(
//                  override val uid: String
//                )
//  extends ProbabilisticClassifier[Vector, NaiveBayes, NaiveBayesModel] {
//
//  override protected def train(dataset: Dataset[_]): NaiveBayesModel = {
//    null
//  }
//
//  override def copy(extra: ParamMap): NaiveBayes = defaultCopy(extra)
//
//}
//
//object NaiveBayes {
//  val Multinomial: String = "multinomial"
//  val Bernoulli: String = "bernoulli"
//
//  private val supportedModelTypes = Set(Multinomial, Bernoulli)
//
//  /**
//    * 检测向量是否含有空值
//    *
//    * @param v
//    */
//  private def requireNonnegativeValues(v: Vector): Unit = {
//    val values = v match {
//      case sv: SparseVector => sv.values
//      case dv: DenseVector => dv.values
//    }
//
//    require(values.forall(_ >= 0.0),
//      s"NavieBayes requires nonnegative feature values but found $v.")
//  }
//
//  /**
//    * 二项式,向量值检测
//    *
//    * @param v
//    */
//  private def requireZeroOneBernoulliValues(v: Vector): Unit = {
//    val values = v match {
//      case sv: SparseVector => sv.values
//      case dv: DenseVector => dv.values
//    }
//
//    require(values.forall(v => v == 0 || v == 1.0),
//      s"Bernoulli navie bayes requires 0 or 1 feature values but found $v.")
//  }
//
//}
//
///**
//  * Model produced by [[NaiveBayes]]
//  * 贝叶斯模型,pi:分类标签值的对数,向量,一位数组,1行,每个元素时一个分类
//  * theta:每组分类的特征值,table结构.行数是分类数,即pi的size大小,列(columns)为纬度信息
//  *
//  * @param pi    log of class priors, whose dimension is C (number of classes)
//  * @param theta log of class conditional probabilities, whose dimension is C (number of classes)
//  *              by D (number of features)
//  *
//  */
//class NaiveBayesModel(
//                       override val uid: String,
//                       val pi: Vector,
//                       val theta: Matrix
//                     )
//  extends ProbabilisticClassificationModel[Vector, NaiveBayesModel] with NaiveBayesParams {
//
//  import NaiveBayes.{Multinomial, Bernoulli}
//
//
//  /**
//    * mllib NaiveBayes is a wrapper of ml implementation currently.
//    * Input labels of mllib could be {-1, +1} and mllib NaiveBayesModel exposes labels,
//    * both of which are different from ml, so we should store the labels sequentially
//    * to be called by mllib. This should be removed when we remove mllib NaiveBayes.
//    */
//  private var oldLabels: Array[Double] = null
//
//  private def setOldLabels(labels: Array[Double]): this.type = {
//    this.oldLabels = labels
//    this
//  }
//
//  private lazy val (thetaMinusNegTheta, negThetaSum) = $(modelType) match {
//    case Multinomial => (None, None)
//    case Bernoulli =>
//      val negTheta = theta.map(value => math.log(1.0 - math.exp(value)))
//      val ones = new DenseVector(Array.fill(theta.numCols) {
//        1.0
//      })
//
//      val thetaMinusNegTheta = theta.map { value =>
//        value - math.log(1.0 - math.exp(value))
//      }
//      (Option(thetaMinusNegTheta), Option(negTheta.multiply(ones)))
//    case _ =>
//      throw new UnknownError(s"Invalid modelType ${$(modelType)}")
//  }
//
//  override val numFeatures: Int = theta.numCols
//
//  override val numClasses: Int = pi.size
//
//  private def multinomialCalculation(features: Vector) = {
//    val prop = theta.multiply(features)
//    BLAS2.axpy(1.0, pi, prop)
//    prop
//  }
//
//  private def bernoulliCalculation(features: Vector) = {
//    features.foreachActive((_, value) =>
//      require(value == 0.0 || value == 1.0,
//        s"Bernoulli naive Bayes requires 0 or 1 feature values, but found $features")
//    )
//
//    val prob = thetaMinusNegTheta.get.multiply(features)
//    BLAS2.axpy(1.0, pi, prob)
//    BLAS2.axpy(1.0, negThetaSum.get, prob)
//    prob
//  }
//
//  override protected def predictRaw(features: Vector): Vector = {
//    $(modelType) match {
//      case Multinomial =>
//        multinomialCalculation(features)
//      case Bernoulli =>
//        bernoulliCalculation(features)
//      case _ =>
//        throw new UnknownError(s"Invalid modelType ${$(modelType)}")
//    }
//  }
//
//  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
//    rawPrediction match {
//      case dv: DenseVector =>
//        var i = 0
//        val size = dv.size
//        val maxLog = dv.values.max
//        while (i < size) {
//          dv.values(i) = math.exp(dv.values(i) - maxLog)
//          i += 1
//        }
//
//        val propSum = dv.values.sum
//        i = 0
//        while (i < size) {
//          dv.values(i) = dv.values(i) / propSum
//          i += 1
//        }
//        dv
//      case sv: SparseVector =>
//        throw new RuntimeException("Unexpected error in NaiveBayesModel:" +
//          " raw2probabilityInPlace encountered SparseVector.")
//    }
//  }
//
//  override def copy(extra: ParamMap): NaiveBayesModel = {
//    copyValues(new NaiveBayesModel(uid, pi, theta).setParent(this.parent), extra)
//  }
//
//  override def toString(): String = {
//    s"NaiveBayesModel (uid=$uid) with ${pi.size} classes"
//  }
//
//}
//
//object NaiveBayesModel {
//
//}

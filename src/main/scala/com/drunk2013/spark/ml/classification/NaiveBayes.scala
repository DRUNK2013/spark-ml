package com.drunk2013.spark.ml.classification

import com.drunk2013.spark.util.{HasWeightCol, PredictorParams}
import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg._
import com.drunk2013.spark.util.Matrix
import org.apache.spark.ml.param.{DoubleParam, Param, ParamValidators}

/**
  * Created by shuangfu on 17-2-14.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */

private[classification] trait NaiveBayesParams extends PredictorParams with HasWeightCol {

  /**
    * the smoothing paramter.
    * default 1.0
    */
  final val smoothing: DoubleParam = new DoubleParam(
    this, "smoothing", "The smoothing parametor.", ParamValidators.gtEq(0))

  final def getSmoothing: Double = $(smoothing)

  /**
    * 贝叶斯模型
    * multinomial:多项分布
    * bernoulli:伯努力分布,即二项式分布
    */
  final val modelType: Param[String] = new Param[String](this, "modelType", "The model type which is a " +
    "string(case sensitive). Supported options:multinomial(default) and bernoulli")

  final def getModelType: String = $(modelType)
}

//class NaiveBayes()
//  extends ProbabilisticClassifier[Vector, NaiveBayes] {
//
//}

object NaiveBayes {
  val Multinomial: String = "multinomial"
  val Bernoulli: String = "bernoulli"

  private val supportedModelTypes = Set(Multinomial, Bernoulli)

  /**
    * 检测向量是否含有空值
    *
    * @param v
    */
  private def requireNonnegativeValues(v: Vector): Unit = {
    val values = v match {
      case sv: SparseVector => sv.values
      case dv: DenseVector => dv.values
    }

    require(values.forall(_ >= 0.0),
      s"NavieBayes requires nonnegative feature values but found $v.")
  }

  private def requireZeroOneBernoulliValues(v: Vector): Unit = {
    val values = v match {
      case sv: SparseVector => sv.values
      case dv: DenseVector => dv.values
    }

    require(values.forall(v => v == 0 || v == 1.0),
      s"Bernoulli navie bayes requires 0 or 1 feature values but found $v.")

  }

}

//class NavieBayesModel private[classification](
//                                               override val uid: String,
//                                               val pi: Vector,
//                                               val theta: Matrix
//                                             )
//  extends ProbabilisticClassificationModel[Vector, NavieBayesModel] with NaiveBayesParams {
//
//  import NaiveBayes.{Multinomial, Bernoulli}
//
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
//  }
//
//}

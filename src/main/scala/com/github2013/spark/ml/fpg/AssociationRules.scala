package com.github2013.spark.ml.fpg

import com.github2013.spark.ml.fpg.AssociationRules.Rule
import com.github2013.spark.ml.fpg.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by shuangfu on 17-1-20.
  */
class AssociationRules private[fpg](
                                     private var minConfidence: Double
                                   ) extends Serializable {

  /*
   配置默认对最小支持度0.8
   */
  def this() = this(0.8)


  /**
    *
    * @param minConfidence 设置支持度参数
    * @return
    */
  def setMinConfidence(minConfidence: Double): this.type = {
    require(minConfidence >= 0.0 && minConfidence <= 1.0,
      s"最小支持度必须在[0,1]之间,传递的值为:${minConfidence}")
    this.minConfidence = minConfidence
    this
  }


  /**
    * 通过频繁数据集项
    *
    * @param freqItemsets 频繁数据集
    * @tparam Item 反射类型
    * @return 返回关联规则信息
    */
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {
    // For candidate rule X => Y, generate (X, (Y, freq(X union Y)))
    val candicates = freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.flatMap { item =>
        items.partition(_ == item) match {
          case (consequent, antecedent) if !antecedent.isEmpty =>
            Some((antecedent.toSeq, (consequent.toSeq, itemset.freq)))
          case _ => None
        }
      }
    }
    // Join to get (X, ((Y, freq(X union Y)), freq(X))), generate rules, and filter by confidence
    candicates.join(freqItemsets.map(x => (x.items.toSeq, x.freq)))
      .map { case (anticedent, ((consequent, freqUnion), freqAnticedent)) =>
        new Rule(anticedent.toArray, consequent.toArray, freqUnion, freqAnticedent)
      }.filter(_.confidence >= minConfidence)
  }
}

object AssociationRules {

  /**
    * 数据集中每个数据项中的关联规则数据结构
    *
    * @param antecedent     前项数据集合
    * @param consequent     后项数据集合
    * @param freqUnion      所有对数据集大小
    * @param freqAntecedent 前项累计数
    * @tparam Item 泛型
    */
  class Rule[Item] private[fpg](
                                 val antecedent: Array[Item],
                                 val consequent: Array[Item],
                                 freqUnion: Double,
                                 freqAntecedent: Double
                               ) extends Serializable {
    def confidence: Double = freqUnion / freqAntecedent

    require(antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet)
      s"数据项不符合关联规则,前项和后项不能有相同元素.错误的重合数据项:${sharedItems}"
    })

    override def toString: String = {
      s"${antecedent.mkString("{", ",", "}")}==>" +
        s"${consequent.mkString("{", ",", "}")}:${confidence}"
    }
  }

}

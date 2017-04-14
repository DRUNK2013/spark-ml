package com.drunk2013.spark.ml.classification

import org.apache.spark.ml.tree.Node

/**
  * Created by shuangfu on 17-3-9.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
/**
  * Abstraction for Decision Tree Model
  * Notice: currently not support predicting probabilities and raw predictions
  */
trait DecisionTreeModel {

  //root of the decision tree
  def rootNode: Node

//  def numNodes: Int = {
//    1 + rootNode.numDescendants
//  }

//  lazy val depth:Int = {
//
//  }

}

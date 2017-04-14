package com.drunk2013.spark.ml.classification
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.{ImpurityStats,
InformationGainStats => OldInformationGainStats, Node => OldNode, Predict => OldPredict}
/**
  * Created by shuangfu on 17-3-9.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
//sealed abstract class TreeNode extends Serializable {
//  // TODO: Add aggregate stats (once available).  This will happen after we move the DecisionTree
//  //       code into the new API and deprecate the old API.  SPARK-3727
//
//  /** Prediction a leaf node makes, or which an internal node would make if it were a leaf node */
//  def prediction: Double
//
//  /** Impurity measure at this node (for training data) */
//  def impurity: Double
//
//  /**
//    * Statistics aggregated from training data at this node, used to compute prediction, impurity,
//    * and probabilities.
//    * For classification, the array of class counts must be normalized to a probability distribution.
//    */
//  private[ml] def impurityStats: ImpurityCalculator
//
//  /** Recursive prediction helper method */
//  private[ml] def predictImpl(features: Vector): LeafNode
//
//  /**
//    * Get the number of nodes in tree below this node, including leaf nodes.
//    * E.g., if this is a leaf, returns 0.  If both children are leaves, returns 2.
//    */
//  private[tree] def numDescendants: Int
//
//  /**
//    * Recursive print function.
//    * @param indentFactor  The number of spaces to add to each level of indentation.
//    */
//  private[tree] def subtreeToString(indentFactor: Int = 0): String
//
//  /**
//    * Get depth of tree from this node.
//    * E.g.: Depth 0 means this is a leaf node.  Depth 1 means 1 internal and 2 leaf nodes.
//    */
//  private[tree] def subtreeDepth: Int
//
//  /**
//    * Create a copy of this node in the old Node format, recursively creating child nodes as needed.
//    * @param id  Node ID using old format IDs
//    */
//  private[ml] def toOld(id: Int): OldNode
//
//  /**
//    * Trace down the tree, and return the largest feature index used in any split.
//    * @return  Max feature index used in a split, or -1 if there are no splits (single leaf node).
//    */
//  private[ml] def maxSplitFeatureIndex(): Int
//
//  /** Returns a deep copy of the subtree rooted at this node. */
//  private[tree] def deepCopy(): TreeNode
//
//}
//
//class LeafNode private[ml] (
//                             override val prediction: Double,
//                             override val impurity: Double,
//                             override private val impurityStats: ImpurityCalculator) extends TreeNode {
//
//  override def toString: String =
//    s"LeafNode(prediction = $prediction, impurity = $impurity)"
//
//  override private[ml] def predictImpl(features: Vector): LeafNode = this
//
//  override private[tree] def numDescendants: Int = 0
//
//  override private[tree] def subtreeToString(indentFactor: Int = 0): String = {
//    val prefix: String = " " * indentFactor
//    prefix + s"Predict: $prediction\n"
//  }
//
//  override private[tree] def subtreeDepth: Int = 0
//
//  override private[ml] def toOld(id: Int): OldNode = {
//    new OldNode(id, new OldPredict(prediction, prob = impurityStats.prob(prediction)),
//      impurity, isLeaf = true, None, None, None, None)
//  }
//
//  override private[ml] def maxSplitFeatureIndex(): Int = -1
//
//  override private[tree] def deepCopy(): TreeNode = {
//    new LeafNode(prediction, impurity, impurityStats)
//  }
//}

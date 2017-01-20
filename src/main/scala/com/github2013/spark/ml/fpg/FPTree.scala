package com.github2013.spark.ml.fpg

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by shuangfu on 17-1-20.
  */
private[fpg] class FPTree[T] extends Serializable {

  import FPTree._

  val root: Node[T] = new Node(null)

//  val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty


}

private[fpg] object FPTree {

  /**
    * 构造FPTree树节点
    *
    * @param parent
    * @tparam T
    */
  class Node[T](val parent: Node[T]) extends Serializable {
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null
  }

  private class Summary[T] extends Serializable {
    val count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }

}

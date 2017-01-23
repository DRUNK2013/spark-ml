package com.drunk2013.spark.ml.fpg

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by shuangfu on 17-1-20.
  */


/**
  * 构造频繁树模型
  *
  * @tparam T
  */
private[fpg] class FPTree[T] extends Serializable {

  import FPTree._

  val root: Node[T] = new Node(null)

  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  /** Merges another FP-Tree. */
  def add(t: Iterable[T], count: Long = 1L): this.type = {
    require(count > 0, s"count 数必须大于0")
    var curr = root
    curr.count += count
    t.foreach { item =>
      val summary = summaries.getOrElseUpdate(item, new Summary)
      summary.count += count
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        summary.nodes += newNode
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: FPTree[T]): this.type = {
    this
  }

  /** Gets a subtree with the suffix. */
  def project(suffix: T): FPTree[T] = {
    val tree = new FPTree[T]
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach { node =>
        var t = List.empty[T]
        var curr = node.parent
        while (!curr.isRoot) {
          t = curr.item :: t
          curr = curr.parent
        }
        tree.add(t, node.count)
      }
    }
    tree
  }

  /** Returns all transactions in an iterator. */
  def transactions: Iterator[(List[T], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single(Nil, count)
      } else {
        Iterator.empty
      }
    }
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(
               minCount: Long,
               validateSuffix: T => Boolean = _ => true
             ): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count > minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }

    }
    null
  }

}

private[fpg] object FPTree {

  /**
    * FPTree数据结构
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
    var count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }

}

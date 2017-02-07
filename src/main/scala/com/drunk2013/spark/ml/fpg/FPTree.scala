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

  //FPTree存储
  val root: Node[T] = new Node(null)

  //各个项统计信息,记录:项值,count,children
  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  /** Adds a transaction with count. */
  /**
    * 传递数据项,返回FPTree,递归添加
    *
    * @param t
    * @param count
    * @return
    */
  def add(t: Iterable[T], count: Long = 1L): this.type = {
    require(count > 0, s"count 数必须大于0")
    var curr = root
    curr.count += count
    //遍历数据项,
    t.foreach { item =>
      //更新项头表,和节点对频次
      val summary = summaries.getOrElseUpdate(item, new Summary)
      summary.count += count

      //更新或添加FPTree子节点
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        // 项表添加新成员对位置
        summary.nodes += newNode
        newNode
      })
      // 更新child的次数
      child.count += count

      //指针向后移动
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: FPTree[T]): this.type = {
    other.transactions.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  /** Gets a subtree with the suffix. */
  /**
    * 根据末节点值,向上搜索,抽取其子树
    * @param suffix
    * @return
    */
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
  /**
    * 把节点树,递归转换成Iterator数组
    *
    * @param node
    * @return
    */
  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count

    println(s"all count:${count}")
    node.children.iterator.flatMap { case (item, child) =>
      println(s"item:${item},child:${child.toString}")
      getTransactions(child).map { case (t, c) =>
        println(s"t:${t},c:${c}")
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
  /**
    * 抽取超过最小频繁数对数据集
    * @param minCount
    * @param validateSuffix
    * @return
    */
  def extract(
               minCount: Long,
               validateSuffix: T => Boolean = _ => true
             ): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }

}

private[fpg] object FPTree {

  /**
    * 描述FPTree中节点数据结构-链表
    * Representing a node in an FP-Tree.
    *
    * @param parent
    * @tparam T
    */
  class Node[T](val parent: Node[T]) extends Serializable {
    //设置item 为 传递进来对T值,即项值
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null

    override def toString: String = s"item:${item},count:${count},children:${children.mkString(",")}"
  }

  /**
    * Summary of an item in an FP-Tree.
    * 项头表，同时也是频繁1项集,并包含相同的位置
    *
    * @tparam T
    */
  private class Summary[T] extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }

}

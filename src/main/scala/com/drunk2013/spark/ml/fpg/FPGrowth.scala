package com.drunk2013.spark.ml.fpg

import java.{util => ju}

import com.drunk2013.spark.ml.fpg.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkException}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by shuangfu on 17-1-20.
  */


class FPGrowth private(private var minSupport: Double,
                       private var numPartitions: Int) extends Serializable {

  def this() = this(0.3, -1)

  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1, s"最小支持度应在[0,1]之间,${minSupport}非法")
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0, s"partition 的数量必须大于0,${numPartitions}非法")
    this.numPartitions = numPartitions
    this
  }

  def run[Item: ClassTag](data: RDD[Array[Item]]): FPGrowthModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      println("Input data is not cache")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    //获取符合频次的项集
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner) //分解找出子项集
    new FPGrowthModel(freqItemsets)
  }

  /**
    * 对原始数据,进行项集统计
    *
    * @param data
    * @param minCount
    * @param partitioner
    * @tparam Item
    * @return 返回项集,按照频次大小降序
    */
  private def genFreqItems[Item: ClassTag](
                                            data: RDD[Array[Item]],
                                            minCount: Long,
                                            partitioner: Partitioner
                                          ): Array[Item] = {
    //每个项集中对项不能重复
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"数据集不能重复,${t.toSeq}非法")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 > minSupport) //过滤小于支持度的项集
      .collect() //聚集成单个数组
      .sortBy(-_._2) //降序
      .map(_._1)
  }

  /**
    * 获取频繁项集,在原始项集中,分解出子项集
    *
    * @param data
    * @param minCount
    * @param freqItems
    * @param partitioner
    * @tparam Item
    * @return
    */
  private def genFreqItemsets[Item: ClassTag](
                                               data: RDD[Array[Item]],
                                               minCount: Long,
                                               freqItems: Array[Item],
                                               partitioner: Partitioner
                                             ): RDD[FreqItemset[Item]] = {
    //对每个原始数据集,进行编号.注意:每个分区都会编号一次,因此每个分区中项集的编号时唯一的.单每个分区分区对对应对项集,会不同.
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner) //构建候选集
    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(//聚合构建FPTree
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2)
    ).flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part) //抽取频繁项集
    }.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count) //频繁集对象
    }
  }

  /**
    * 生成候选集,把原始数据,进行切分,
    *
    * @param transaction
    * @param itemToRank
    * @param partitioner
    * @tparam Item
    * @return
    */
  private def genCondTransactions[Item: ClassTag](
                                                   transaction: Array[Item],
                                                   itemToRank: Map[Item, Int],
                                                   partitioner: Partitioner
                                                 ): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    //val filtered2 = transaction.flatMap(x => itemToRank.get(x))
    val filtered = transaction.flatMap(itemToRank.get) //获取每个项集对编号
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item) //通过partition
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1) //切分候选集
      }
      i -= 1
    }
    output
  }


}

object FPGrowth {

  /*
  FreqItemset 结构体,Item为泛型参数
   */
  class FreqItemset[Item](
                           val items: Array[Item],
                           val freq: Long
                         ) extends Serializable {
    override def toString: String = {
      s"${items.mkString("{", ",", "}")}:$freq"
    }

  }

}

/**
  * 通过关联规则,过滤置信度
  *
  * @param freqItemsets
  * @tparam Item
  */
class FPGrowthModel[Item: ClassTag](
                                     val freqItemsets: RDD[FreqItemset[Item]]
                                   ) extends Serializable {
  def generateAssociationRules(confidence: Double): RDD[AssociationRules.Rule[Item]] = {
    val associationRules = new AssociationRules(confidence)
    associationRules.run(freqItemsets)
  }

}


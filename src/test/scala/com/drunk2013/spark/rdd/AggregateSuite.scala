package com.drunk2013.spark.rdd

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

import scala.collection.mutable

/**
  * Created by shuangfu on 17-2-7.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
object TreeAggregate {
  def seq(a: Int, b: Int): Int = {
    println("seq=a:" + a + ",b:" + b)
    math.max(a, b)
  }

  def comb(a: Int, b: Int): Int = {
    println("comb=a:" + a + ",b:" + b)
    a + b
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_ml_src")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(1, 2, 3, 4, 5, 8, 9), 2)
    println("====================aggregate=======================")
    val resultAggregate = data.aggregate(0)(seq, comb)
    println(resultAggregate)

    println("==================treeAggregate====================")
    val resultTreeAggregate = data.treeAggregate(0)(seq, comb)
    println(resultTreeAggregate)

    /**
      * Aggregate the elements of each partition, and then the results for all the partitions,
      * using given combine functions and a neutral "zero value".
      * This function can return a different result type, U, than the type of this RDD, T.
      * Thus, we need one operation for merging a T into an U and one operation for merging two U's, as in scala.TraversableOnce.
      * Both of these functions are allowed to modify and return their first argument instead of creating a new U to avoid memory allocation.
      */
    val inputrdd = sc.parallelize(
      List(
        ("maths", 21),
        ("english", 22),
        ("science", 31)
      ), 3)

    println("partition size:" + inputrdd.partitions.size)
    val zeroValue = 0
    val result = inputrdd.treeAggregate(zeroValue)(
      //    val result = inputrdd.aggregate(zeroValue)(
      /*
      * This is a seqOp for merging T into a U
      * ie (String, Int) in  into Int
      * (we take (String, Int) in 'value' & return Int)
      * Arguments :
      * acc   :  Represents the accumulated result
      * value :  Represents the element in 'inputrdd'
      *          In our case this of type (String, Int)
      * Return value
      * We are returning an Int
      * value 和 inputrdd里数据类型一致
      * acc 是累积的值
      * acc1,acc2是各个分区再次聚合,类型一致.都和Value类型一致
      */

      (acc, value) => (acc + value._2),

      /*
         * This is a combOp for mergining two U's
         * (ie 2 Int)
         */
      (acc1, acc2) => (acc1 + acc2)
    )

    println("=================各个科目成绩累积=================")
    println(result)

    println("============新聚合aggregate===============")
    val scoreRDD = sc.parallelize(
      List(
        ("zsf", "maths", 90),
        ("jy", "maths", 80),
        ("wzm", "maths", 70),
        ("zsf", "english", 70),
        ("jy", "english", 90),
        ("wzm", "english", 80),
        ("zsf", "compute", 80),
        ("jy", "compute", 90),
        ("wzm", "compute", 70)
      ), 2)
    println("partition size:" + scoreRDD.partitions.size)
    val studentAggregator = new StudentAggregator
    val scoreResult = scoreRDD.aggregate(studentAggregator)(
      (sa, v) => (sa.add(new Student(v._1, v._3))),
      (sa1, sa2) => (sa1.merge(sa2))
    )
    scoreRDD.foreachPartition(iter => {

    })
    scoreResult.listScore().foreach(println(_))
  }

}

case class Student(name: String, score: Int)

class StudentAggregator() extends Serializable {
  private var nameScore: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]

  /**
    * 添加学生或更新成绩
    *
    * @param student
    * @return
    */
  def add(student: Student): this.type = {
    val score = nameScore.getOrElse(student.name, 0)
    nameScore.update(student.name, score + student.score)
    this
  }

  /**
    * 对多个StudentAggregator进行合并
    *
    * @param other
    * @return
    */
  def merge(other: StudentAggregator): this.type = {
    val nameScoreMerge = this.nameScore
    for ((key, value) <- other.nameScore) {
      val score = nameScore.getOrElse(key, 0)
      nameScore.update(key, score + value)
    }
    this
  }

  def listScore(): mutable.HashMap[String, Int] = {
    this.nameScore
  }

}

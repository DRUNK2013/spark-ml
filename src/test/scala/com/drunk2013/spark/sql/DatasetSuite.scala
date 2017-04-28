package com.drunk2013.spark.sql


import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{ClassData, NonSerializableCaseClass}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.functions._

/**
  * Created by shuangfu on 17-4-21.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
class DatasetSuite extends SharedSQLContext {

  import testImplicits._

  test("checkAnswer should compare map correctly") {
    val data = Seq((1, "2", Map(1 -> 2, 2 -> 1)))
    val df = data.toDF()
    df.printSchema()
    df.show
  }

  test("toDS") {
    val data = Seq(("a", 1), ("b", 2), ("c", 3))
    val ds = data.toDS()
    ds.printSchema()
    ds.show()
  }

  test("toDS by RDD") {
    val ds = sparkContext.makeRDD(Seq("a", "b", "c"), 4).toDS
    val ret = ds.mapPartitions(partition => {
      println(partition)
      partition.foreach(println)
      Iterator(1)
    })
    ret.show()
    println(ret)
  }

  test("emptyDataset") {
    val ds = spark.emptyDataset[Int]
    assert(ds.count() == 0L)
    assert(ds.collect() sameElements Array.empty[Int])
  }

  test("range") {
    assert(spark.range(10).map(_ + 1).reduce(_ + _) == 55)
    assert(spark.range(10).map { case i: java.lang.Long => i + 1 }.reduce(_ + _) == 55)
    assert(spark.range(0, 10).map(_ + 1).reduce(_ + _) == 55)
    assert(spark.range(0, 10).map { case i: java.lang.Long => i + 1 }.reduce(_ + _) == 55)
    assert(spark.range(0, 10, 1, 2).map(_ + 1).reduce(_ + _) == 55)
    assert(spark.range(0, 10, 1, 2).map { case i: java.lang.Long => i + 1 }.reduce(_ + _) == 55)
  }

  test("Datatype Helper Serializability") {
    val ds = sparkContext.parallelize((
      new Timestamp(0),
      new Date(0),
      java.math.BigDecimal.valueOf(1),
      scala.math.BigDecimal(1)) :: Nil).toDS()
    ds.collect()
  }

  test("collect, first, and take should use encoders for serialization") {
    val item = NonSerializableCaseClass("abcd")
    val ds = Seq(item).toDS()
    assert(ds.collect().head == item)
    assert(ds.collectAsList().get(0) == item)
    assert(ds.first() == item)
    assert(ds.take(1).head == item)
    assert(ds.takeAsList(1).get(0) == item)
    assert(ds.toLocalIterator().next() == item)
  }

  test("coalesce,reparttion") {
    val data = (1 to 100).map(i => ClassData(i.toString, i))
    val ds = data.toDS()

    intercept[IllegalArgumentException] {
      ds.coalesce(0)
    }

    intercept[IllegalArgumentException] {
      ds.repartition(0)
    }

    assert(ds.repartition(10).rdd.partitions.length == 10)
    assert(ds.coalesce(1).rdd.partitions.length == 1)

  }

  test("as tuple") {
    val data = Seq(("a", 1), ("b", 2)).toDF("a", "b")
  }

  test("test map") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    ds.map(v => (v._1, v._2 + 1)).show()
  }

  test("typed filter should preserve the underlying logical schema") {
    val ds = spark.range(10)
    val ds2 = ds.filter(_ > 3)

    assert(ds.schema.equals(ds2.schema))
  }

  test("foreach") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.longAccumulator
    ds.foreach(v => acc.add(v._2))
    assert(acc.value == 6)
  }

  test("foreachPartition") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    val acc = sparkContext.longAccumulator
    ds.foreachPartition((_.foreach(v => acc.add(v._2))))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(("a", 1), ("b", 2), ("c", 3)).toDS()
    assert(ds.reduce((a, b) => ("sum", a._2 + b._2)) == ("sum", 6))
  }

  test("joinWith, flat schema") {
    val ds1 = Seq(1, 2, 3).toDS().as("a")
    val ds2 = Seq(1, 2).toDS().as("b")
  }

  test("groupBy function, flatMap") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    val grouped = ds.groupByKey(v => (v._1, "word"))
    val agged = grouped.mapGroups { case (g, iter) =>
      (g._1, iter.map(_._2).sum)
    }
    agged.show()
  }

  test("typed aggregation:expr,expr") {
    val ds = Seq(("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)).toDS()
    ds.groupByKey(_._1).agg(sum("_2").as[Long])
      .show

    ds.groupByKey(_._1).agg(
      sum("_2").as[Long],
      sum($"_2" + 1).as[Long],
      count("*").as[Long],
      avg("_2").as[Double]
    ).show

  }

  test("estimating sizeInBytes in operators with ObjectProducer shouldn't fail") {
    val dataset = Seq(
      (0, 3, 54f),
      (0, 4, 44f),
      (0, 5, 42f),
      (1, 3, 39f),
      (1, 5, 33f),
      (1, 4, 26f),
      (2, 3, 51f),
      (2, 5, 45f),
      (2, 4, 30f)
    ).toDF("user", "item", "rating")

    val actual = dataset.select("user", "item")
      .as[(Int, Int)]
      .groupByKey(_._1)
      .mapGroups { (src, ids) => (src, ids.map(_._2).toArray) }
      .toDF("id", "actual")
    actual.printSchema()
    actual.show
  }


}

package com.drunk2013.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

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
  }


}

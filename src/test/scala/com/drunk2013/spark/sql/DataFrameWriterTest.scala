package com.drunk2013.spark.sql

import java.util.Properties

import org.apache.spark.util.Utils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by shuangfu on 17-2-3.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
class DataFrameWriterTest extends SharedSQLContext {

  test("testJdbc") {
    val url = "jdbc:mysql:192.168.1.100:test"
    var conn: java.sql.Connection = null
    val properties = new Properties()
    properties.setProperty("user", "test")
    properties.setProperty("password", "test")
    properties.setProperty("rowId", "false")
    properties.setProperty("drive", "com.mysql.jdbc.Driver")
    //    Utils.classForName("org.h2.Driver")
//    Class.forName("com.mysql.jdbc.Driver")

    val arr3x2 = Array[Row](Row.apply("zsf", 88), Row.apply("jy", 93), Row.apply("wzm", 77))
    val schema2 = StructType(
      StructField("name", StringType) ::
        StructField("score", IntegerType) :: Nil)

    val df = spark.createDataFrame(sparkContext.parallelize(arr3x2), schema2)

    df.show(100)
    //    df.write.jdbc(url, "student", properties)
    //    assert(2 === spark.read.jdbc(url, "TEST.BASICCREATETEST", new Properties()).collect()(0).length)
  }

}

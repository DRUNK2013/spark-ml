package com.drunk2013.spark.sql

import java.util.Properties

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.command.DDLUtils

import scala.collection.JavaConverters._
import org.apache.spark.sql.{Dataset, SaveMode}

import scala.collection.mutable

/**
  * Created by shuangfu on 17-2-3.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */

final class DataFrameWriter[T](ds: Dataset[T]) {

  //把dataset转换成dataframe
  private val df = ds.toDF()

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options,定义属性配置
  ///////////////////////////////////////////////////////////////////////////////////////

  //  private var source: String = df.sparkSession.sessionState.conf.defaultDataSourceName
  private var source: String = "parquet"
  private var mode: SaveMode = SaveMode.ErrorIfExists
  private var extraOptions = new mutable.HashMap[String, String]
  private var partitioningColumns: Option[Seq[String]] = None
  private var bucketColumnNames: Option[Seq[String]] = None
  private var numBuckets: Option[Int] = None
  private var sortColumnNames: Option[Seq[String]] = None


  /** 设定dataFrame写入对格式,
    * Specifies the behavior when data or table already exists. Options include:
    *   - `SaveMode.Overwrite`: overwrite the existing data.重写,表先trncate,再新建重写
    *   - `SaveMode.Append`: append the data.
    *   - `SaveMode.Ignore`: ignore the operation (i.e. no-op).
    *   - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.默认方式
    *
    * @since 1.4.0
    */
  def mode(saveMode: SaveMode): DataFrameWriter[T] = {
    this.mode = saveMode
    this
  }

  /**
    * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
    *
    * @since 1.4.0
    */
  /**
    * 设置数据对村村格式
    *
    * @param source
    * @return
    */
  def formart(source: String): DataFrameWriter[T] = {
    this.source = source
    this
  }

  /**
    * 数据写入添加属性
    *
    * @param key   ,字符词类型
    * @param value ,可以是字符串,整数,小数,布尔类型
    * @return
    */
  def option(key: String, value: Any): DataFrameWriter[T] = {
    this.extraOptions += (key -> value.toString)
    this
  }

  def options(options: scala.collection.Map[String, String]): DataFrameWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
    * Partitions the output by the given columns on the file system. If specified, the output is
    * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
    * partition a dataset by year and then month, the directory layout would look like:
    *
    *   - year=2016/month=01/
    *   - year=2016/month=02/
    *
    * Partitioning is one of the most widely used techniques to optimize physical data layout.
    * It provides a coarse-grained index for skipping unnecessary data reads when queries have
    * predicates on the partitioned columns. In order for partitioning to work well, the number
    * of distinct values in each column should typically be less than tens of thousands.
    *
    * This is applicable for all file-based data sources (e.g. Parquet, JSON) staring Spark 2.1.0.
    *
    * @since 1.4.0
    *        设置数据分区,为了体现此分区的性能优势,请保证列去重后对值,在万条以内.
    * @param colNames
    * @return
    */
  def partitionBy(colNames: String*): DataFrameWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
    * Buckets the output by the given columns. If specified, the output is laid out on the file
    * system similar to Hive's bucketing scheme.
    *
    * This is applicable for all file-based data sources (e.g. Parquet, JSON) staring Spark 2.1.0.
    *
    * @since 2.0
    *
    *        shuffe桶的数目,桶内对数据,必须时一样的.,为了体现此分区的性能优势,请保证列去重后对值,在万条以内.
    * @param numBuckets
    * @param colName
    * @param colNames
    * @return
    */
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter[T] = {
    this.numBuckets = Option(numBuckets)
    this.bucketColumnNames = Option(colName +: colNames)
    this
  }

  /**
    * Sorts the output in each bucket by the given columns.
    *
    * This is applicable for all file-based data sources (e.g. Parquet, JSON) staring Spark 2.1.0.
    * 在输出对文件桶中,对桶内对数据进行排序
    *
    * @param colName
    * @param colNames
    * @return
    */
  def sortBy(colName: String, colNames: String*): DataFrameWriter[T] = {
    this.sortColumnNames = Option(colName +: colNames)
    this
  }

  private def assertNotBucketed(operation: String): Unit = {
    if (numBuckets.isDefined || sortColumnNames.isDefined) {
      throw new SparkException(s"'$operation' does not support bucketing right now")
    }
  }

  private def assertNotPartitioned(operation: String): Unit = {
    if (partitioningColumns.isDefined) {
      throw new SparkException(s"'$operation' does not support partitioning")
    }
  }

  /**
    * Saves the content of the `DataFrame` to an external database table via JDBC. In the case the
    * table already exists in the external database, behavior of this function depends on the
    * save mode, specified by the `mode` function (default to throwing an exception).
    *
    * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
    * your external database systems.
    *
    * You can set the following JDBC-specific option(s) for storing JDBC:
    *
    * <li>`truncate` (default `false`): use `TRUNCATE TABLE` instead of `DROP TABLE`.</li>
    * </ul>
    *
    * In case of failures, users should turn off `truncate` option to use `DROP TABLE` again. Also,
    * due to the different behavior of `TRUNCATE TABLE` among DBMS, it's not always safe to use this.
    * MySQLDialect, DB2Dialect, MsSqlServerDialect, DerbyDialect, and OracleDialect supports this
    * while PostgresDialect and default JDBCDirect doesn't. For unknown and unsupported JDBCDirect,
    * the user option `truncate` is ignored.
    *
    * @param url                  JDBC database url of the form `jdbc:subprotocol:subname`
    * @param table                Name of the table in the external database.
    * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
    *                             tag/value. Normally at least a "user" and "password" property
    *                             should be included. "batchsize" can be used to control the
    *                             number of rows per insert. "isolationLevel" can be one of
    *                             "NONE", "READ_COMMITTED", "READ_UNCOMMITTED", "REPEATABLE_READ",
    *                             or "SERIALIZABLE", corresponding to standard transaction
    *                             isolation levels defined by JDBC's Connection object, with default
    *                             of "READ_UNCOMMITTED".
    *                             数据写入到数据库中,不要把partition的数目设置对过大,否则会把数据服务器写宕机
    *                             connectionProperties参数:有batchsize,isolationLevel等
    * @since 1.4.0
    */
  def jdbc(url: String, table: String, connectionProperties: Properties): Unit = {
    assertNotPartitioned("jdbc")
    assertNotBucketed("jdbc")
    // connectionProperties should override settings in extraOptions.
    //把java的properties对象转换成scala对象
    this.extraOptions ++= connectionProperties.asScala
    this.extraOptions += ("url" -> url, "dbtable" -> table)
    formart("jdbc").save()
  }


  def save(path: String): Unit = {
    this.extraOptions += ("path" -> path)
    save()
  }

  def save(): Unit = {
    if (source.toLowerCase == DDLUtils.HIVE_PROVIDER) {
      throw new SparkException("Hive data source can only be used with tables, you can not " +
        "write files of Hive data source directly.")
    }
    assertNotBucketed("save")

    val dataSource = new DataSource(
      df.sparkSession,
      className = source,
      partitionColumns = partitioningColumns.getOrElse(Nil),
//      bucketSpec = getBucketSpec,
      options = extraOptions.toMap
    )
    dataSource.write(mode,df)

  }

//  private def getBucketSpec: Option[BucketSpec] = {
//    if (sortColumnNames.isDefined) {
//      require(numBuckets.isDefined, "sortBy must be used together with bucketBy")
//    }
//
//    for {
//      n <- numBuckets
//    } yield {
//      require(n > 0 && n < 100000, "Bucket number must be greater than 0 and less than 100000.")
//
//      // partitionBy columns cannot be used in bucketBy
//      if (normalizedParCols.nonEmpty &&
//        normalizedBucketColNames.get.toSet.intersect(normalizedParCols.get.toSet).nonEmpty) {
//        throw new AnalysisException(
//          s"bucketBy columns '${bucketColumnNames.get.mkString(", ")}' should not be part of " +
//            s"partitionBy columns '${partitioningColumns.get.mkString(", ")}'")
//      }
//
//      BucketSpec(n, normalizedBucketColNames.get, normalizedSortColNames.getOrElse(Nil))
//    }
//  }
//
//  private def normalizedParCols: Option[Seq[String]] = partitioningColumns.map { cols =>
//    cols.map(normalize(_, "Partition"))
//  }
//
//  private def normalizedBucketColNames: Option[Seq[String]] = bucketColumnNames.map { cols =>
//    cols.map(normalize(_, "Bucketing"))
//  }
//
//  private def normalizedSortColNames: Option[Seq[String]] = sortColumnNames.map { cols =>
//    cols.map(normalize(_, "Sorting"))
//  }
//
//  /**
//    * The given column name may not be equal to any of the existing column names if we were in
//    * case-insensitive context. Normalize the given column name to the real one so that we don't
//    * need to care about case sensitivity afterwards.
//    */
//  private def normalize(columnName: String, columnType: String): String = {
//    val validColumnNames = df.logicalPlan.output.map(_.name)
//    validColumnNames.find(df.sparkSession.sessionState.analyzer.resolver(_, columnName))
//      .getOrElse(throw new AnalysisException(s"$columnType column $columnName not found in " +
//        s"existing columns (${validColumnNames.mkString(", ")})"))
//  }
}

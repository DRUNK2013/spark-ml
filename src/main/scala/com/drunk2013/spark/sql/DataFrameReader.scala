package com.drunk2013.spark.sql

import java.util.Properties

import scala.collection.JavaConverters._
import com.drunk2013.spark.util.Logging
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/**
  * Created by shuangfu on 17-2-4.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
class DataFrameReader private[sql](sparkSession: SparkSession) extends Logging {

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  // 设置对象属性
  ///////////////////////////////////////////////////////////////////////////////////////
  //  private var source: String = df.sparkSession.sessionState.conf.defaultDataSourceName
  private var source: String = "parquet"
  private var userSpecifiedSchema: Option[StructType] = None
  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

  /**
    * Specifies the input data source format.
    * 设置读取数据的格式
    *
    * @since 1.4.0
    */
  def format(source: String): DataFrameReader = {
    this.source = source
    this
  }

  /**
    * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
    * automatically from data. By specifying the schema here, the underlying data source can
    * skip the schema inference step, and thus speed up data loading.
    *
    * @since 1.4.0
    */
  def schema(schema: StructType): this.type = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
    * Adds an input option for the underlying data source.
    *
    * @since 1.4.0
    */
  def option(key: String, value: String): this.type = {
    this.extraOptions += (key -> value)
    this
  }

  /**
    * Adds an input option for the underlying data source.
    *
    * @since 2.0.0
    */
  def option(key: String, value: Any): DataFrameReader = option(key, value.toString)

  def options(options: scala.collection.Map[String, String]): this.type = {
    this.extraOptions ++= options
    this
  }

  def options(options: java.util.Map[String, String]): this.type = {
    this.options(options.asScala)
    this
  }

  def load(): DataFrame = {
    load(Seq.empty: _*)
  }

  def load(path: String): DataFrame = {
    option("path", path).load(Seq.empty: _*)
  }

  def load(paths: String*): DataFrame = {
    if (source.toLowerCase == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables,you can not " +
        "read files of Hive data source directory.")
    }

    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }


  /**
    * Construct a `DataFrame` representing the database table accessible via JDBC URL
    * url named table and connection properties.
    *
    * @since 1.4.0
    */
  def jdbc(url: String, table: String, properties: Properties): DataFrame = {
    //properties should override settings in extraOptions
    this.extraOptions ++= properties.asScala
    //explicit url and dbtable should override all
    this.extraOptions += (JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table)

    format("jdbc").load()
  }

  /**
    * Construct a `DataFrame` representing the database table accessible via JDBC URL
    * url named table. Partitions of the table will be retrieved in parallel based on the parameters
    * passed to this function.
    *
    * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
    * your external database systems.
    *
    * @param url                  JDBC database url of the form `jdbc:subprotocol:subname`.
    * @param table                Name of the table in the external database.
    * @param columnName           the name of a column of integral type that will be used for partitioning.
    * @param lowerBound           the minimum value of `columnName` used to decide partition stride.
    * @param upperBound           the maximum value of `columnName` used to decide partition stride.
    * @param numPartitions        the number of partitions. This, along with `lowerBound` (inclusive),
    *                             `upperBound` (exclusive), form partition strides for generated WHERE
    *                             clause expressions used to split the column `columnName` evenly. When
    *                             the input is less than 1, the number is set to 1.
    * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
    *                             tag/value. Normally at least a "user" and "password" property
    *                             should be included. "fetchsize" can be used to control the
    *                             number of rows per fetch.
    *数据写入到数据库中,不要把partition的数目设置对过大,否则会把数据服务器写宕机
    *connectionProperties参数:有batchsize,isolationLevel等
    * @since 1.4.0
    */
  def jdbc(
            url: String,
            table: String,
            columnName: String,
            lowerBound: Long,
            upperBound: Long,
            numPartitions: Int,
            connectionProperties: Properties
          ): DataFrame = {
    this.extraOptions ++= Map(
      JDBCOptions.JDBC_PARTITION_COLUMN -> columnName,
      JDBCOptions.JDBC_LOWER_BOUND -> lowerBound.toString,
      JDBCOptions.JDBC_UPPER_BOUND -> upperBound.toString,
      JDBCOptions.JDBC_NUM_PARTITIONS -> numPartitions.toString
    )
    jdbc(url, table, connectionProperties)
  }

}

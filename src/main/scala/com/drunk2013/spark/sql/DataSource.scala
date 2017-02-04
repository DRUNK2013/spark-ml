package com.drunk2013.spark.sql

import java.util.{ServiceConfigurationError, ServiceLoader}

import scala.collection.JavaConverters._
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{CalendarIntervalType, StructType}
import org.apache.spark.util.Utils

/**
  * Created by shuangfu on 17-2-3.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
/**
  * The main class responsible for representing a pluggable Data Source in Spark SQL. In addition to
  * acting as the canonical set of parameters that can describe a Data Source, this class is used to
  * resolve a description to a concrete implementation that can be used in a query plan
  * (either batch or streaming) or to write out data using an external library.
  *
  * From an end user's perspective a DataSource description can be created explicitly using
  * [[org.apache.spark.sql.DataFrameReader]] or CREATE TABLE USING DDL.  Additionally, this class is
  * used when resolving a description from a metastore to a concrete implementation.
  *
  * Many of the arguments to this class are optional, though depending on the specific API being used
  * these optional arguments might be filled in during resolution using either inference or external
  * metadata.  For example, when reading a partitioned table from a file system, partition columns
  * will be inferred from the directory layout even if they are not specified.
  *
  * @param paths               A list of file system paths that hold data.  These will be globbed before and
  *              qualified. This option only works when reading from a [[org.apache.spark.sql.execution.datasources.FileFormat]].
  * @param userSpecifiedSchema An optional specification of the schema of the data. When present
  *                            we skip attempting to infer the schema.
  * @param partitionColumns    A list of column names that the relation is partitioned by. This list is
  *                            generally empty during the read path, unless this DataSource is managed
  *                            by Hive. In these cases, during `resolveRelation`, we will call
  *                            `getOrInferFileFormatSchema` for file based DataSources to infer the
  *                         partitioning. In other cases, if this list is empty, then this table
  *                            is unpartitioned.
  * @param bucketSpec          An optional specification for bucketing (hash-partitioning) of the data.
  * @param catalogTable        Optional catalog table reference that can be used to push down operations
  *                            over the datasource to the catalog service.
  */
case class DataSource(
                       sparkSession: SparkSession,
                       className: String,
                       paths: Seq[String] = Nil,
                       userSpecifiedSchema: Option[StructType] = None,
                       partitionColumns: Seq[String] = Seq.empty,
                       bucketSpec: Option[BucketSpec] = None,
                       options: Map[String, String] = Map.empty,
                       catalogTable: Option[CatalogTable] = None
                     ) {

  lazy val providingClass: Class[_] = DataSource.lookupDataSource(className)
  private val caseInsensitiveOptions = new CaseInsensitiveMap(options)

  /**
    * Writes the given [[DataFrame]] out to this [[DataSource]].
    */
  def write(mode: SaveMode, data: DataFrame): Unit = {
    if (data.schema.map(_.dataType).exists(_.isInstanceOf[CalendarIntervalType])) {
      throw new SparkException("Cannot save interval data type into external storage.")
    }

    providingClass.newInstance() match {
      case dataSource: CreatableRelationProvider =>
        dataSource.createRelation(sparkSession.sqlContext, mode, caseInsensitiveOptions, data)
      case _ =>
        sys.error(s"${providingClass.getCanonicalName} does not allow create table as select.")
    }
  }
}

object DataSource {

  /** A map to maintain backward compatibility in case we move data sources around. */
  private val backwardCompatibilityMap: Map[String, String] = {
    val jdbc = classOf[JdbcRelationProvider].getCanonicalName
    val json = classOf[JsonFileFormat].getCanonicalName
    val parquet = classOf[ParquetFileFormat].getCanonicalName
    val csv = classOf[CSVFileFormat].getCanonicalName
    val libsvm = "org.apache.spark.ml.source.libsvm.LibSVMFileFormat"
    val orc = "org.apache.spark.sql.hive.orc.OrcFileFormat"

    Map(
      "org.apache.spark.sql.jdbc" -> jdbc,
      "org.apache.spark.sql.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc.DefaultSource" -> jdbc,
      "org.apache.spark.sql.execution.datasources.jdbc" -> jdbc,
      "org.apache.spark.sql.json" -> json,
      "org.apache.spark.sql.json.DefaultSource" -> json,
      "org.apache.spark.sql.execution.datasources.json" -> json,
      "org.apache.spark.sql.execution.datasources.json.DefaultSource" -> json,
      "org.apache.spark.sql.parquet" -> parquet,
      "org.apache.spark.sql.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet" -> parquet,
      "org.apache.spark.sql.execution.datasources.parquet.DefaultSource" -> parquet,
      "org.apache.spark.sql.hive.orc.DefaultSource" -> orc,
      "org.apache.spark.sql.hive.orc" -> orc,
      "org.apache.spark.ml.source.libsvm.DefaultSource" -> libsvm,
      "org.apache.spark.ml.source.libsvm" -> libsvm,
      "com.databricks.spark.csv" -> csv
    )
  }

  /**
    * Class that were removed in Spark 2.0. Used to detect incompatibility libraries for Spark 2.0.
    */
  private val spark2RemovedClasses = Set(
    "org.apache.spark.sql.DataFrame",
    "org.apache.spark.sql.sources.HadoopFsRelationProvider",
    "org.apache.spark.Logging")

  /** Given a provider name, look up the data source class definition. */
  def lookupDataSource(provider: String): Class[_] = {
    val provider1 = backwardCompatibilityMap.getOrElse(provider, provider)
    val provider2 = s"$provider1.DefaultSource"
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[DataSourceRegister], loader)

    try {
      serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider1)).toList match {
        // the provider format did not match any given registered aliases
        case Nil =>
          try {
            Try(loader.loadClass(provider1)).orElse(Try(loader.loadClass(provider2))) match {
              case Success(dataSource) =>
                // Found the data source using fully qualified path
                dataSource
              case Failure(error) =>
                if (provider1.toLowerCase == "orc" ||
                  provider1.startsWith("org.apache.spark.sql.hive.orc")) {
                  throw new AnalysisException(
                    "The ORC data source must be used with Hive support enabled")
                } else if (provider1.toLowerCase == "avro" ||
                  provider1 == "com.databricks.spark.avro") {
                  throw new AnalysisException(
                    s"Failed to find data source: ${provider1.toLowerCase}. Please find an Avro " +
                      "package at http://spark.apache.org/third-party-projects.html")
                } else {
                  throw new ClassNotFoundException(
                    s"Failed to find data source: $provider1. Please find packages at " +
                      "http://spark.apache.org/third-party-projects.html",
                    error)
                }
            }
          } catch {
            case e: NoClassDefFoundError => // This one won't be caught by Scala NonFatal
              // NoClassDefFoundError's class name uses "/" rather than "." for packages
              val className = e.getMessage.replaceAll("/", ".")
              if (spark2RemovedClasses.contains(className)) {
                throw new ClassNotFoundException(s"$className was removed in Spark 2.0. " +
                  "Please check if your library is compatible with Spark 2.0", e)
              } else {
                throw e
              }
          }
        case head :: Nil =>
          // there is exactly one registered alias
          head.getClass
        case sources =>
          // There are multiple registered aliases for the input
          sys.error(s"Multiple sources found for $provider1 " +
            s"(${sources.map(_.getClass.getName).mkString(", ")}), " +
            "please specify the fully qualified class name.")
      }
    } catch {
      case e: ServiceConfigurationError if e.getCause.isInstanceOf[NoClassDefFoundError] =>
        // NoClassDefFoundError's class name uses "/" rather than "." for packages
        val className = e.getCause.getMessage.replaceAll("/", ".")
        if (spark2RemovedClasses.contains(className)) {
          throw new ClassNotFoundException(s"Detected an incompatible DataSourceRegister. " +
            "Please remove the incompatible library from classpath or upgrade it. " +
            s"Error: ${e.getMessage}", e)
        } else {
          throw e
        }
    }
  }

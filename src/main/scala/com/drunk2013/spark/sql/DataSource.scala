package com.drunk2013.spark.sql

import java.util.{ServiceConfigurationError, ServiceLoader}

import com.drunk2013.spark.util.{Logging, Utils}
import java.util.{ServiceConfigurationError, ServiceLoader}

import scala.collection.JavaConverters._
import scala.language.{existentials, implicitConversions}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FileFormat, HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{CalendarIntervalType, StructType}

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
  *                            定义DataSource的结构体:
  * 1.包括SparkSesion数据,
  * 2.className:数据类型,jdbc,json,cvs,orc,parquet,.etc
  * 3.paths:text,orc,parquet数据存储对路径
  * 4.userSpecifiedSchema:数据结构schema
  * 5.partitionColumns
  * 6.bucketSpec:桶信息
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
                     ) extends Logging {

  lazy val providingClass: Class[_] = DataSource.lookupDataSource(className)
  private val caseInsensitiveOptions = new CaseInsensitiveMap(options)

  /**
    * Writes the given [[org.apache.spark.sql.DataFrame]] out to this [[DataSource]].
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

  /**
    * Create a resolved [[BaseRelation]] that can be used to read data from or write data into this
    * [[DataSource]]
    *
    * @param checkFilesExist Whether to confirm that the files exist when generating the
    *                        non-streaming file based datasource. StructuredStreaming jobs already
    *                        list file existence, and when generating incremental jobs, the batch
    *                        is considered as a non-streaming file based data source. Since we know
    *                        that files already exist, we don't need to check them again.
    */
  def resolveRelation(checkFilesExist: Boolean = true): BaseRelation = {
    //    val relation = (providingClass.newInstance(), userSpecifiedSchema) match {
    //      // TODO: Throw when too much is given.
    //      case (dataSource: SchemaRelationProvider, Some(schema)) =>
    //        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions, schema)
    //      case (dataSource: RelationProvider, None) =>
    //        dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
    //      case (_: SchemaRelationProvider, None) =>
    //        throw new AnalysisException(s"A schema needs to be specified when using $className.")
    //      case (dataSource: RelationProvider, Some(schema)) =>
    //        val baseRelation =
    //          dataSource.createRelation(sparkSession.sqlContext, caseInsensitiveOptions)
    //        if (baseRelation.schema != schema) {
    //          throw new AnalysisException(s"$className does not allow user-specified schemas.")
    //        }
    //        baseRelation
    //
    //      // We are reading from the results of a streaming query. Load files from the metadata log
    //      // instead of listing them using HDFS APIs.
    //      case (format: FileFormat, _)
    //        if hasMetadata(caseInsensitiveOptions.get("path").toSeq ++ paths) =>
    //        val basePath = new Path((caseInsensitiveOptions.get("path").toSeq ++ paths).head)
    //        val fileCatalog = new MetadataLogFileIndex(sparkSession, basePath)
    //        val dataSchema = userSpecifiedSchema.orElse {
    //          format.inferSchema(
    //            sparkSession,
    //            caseInsensitiveOptions,
    //            fileCatalog.allFiles())
    //        }.getOrElse {
    //          throw new AnalysisException(
    //            s"Unable to infer schema for $format at ${fileCatalog.allFiles().mkString(",")}. " +
    //              "It must be specified manually")
    //        }
    //
    //        HadoopFsRelation(
    //          fileCatalog,
    //          partitionSchema = fileCatalog.partitionSchema,
    //          dataSchema = dataSchema,
    //          bucketSpec = None,
    //          format,
    //          caseInsensitiveOptions)(sparkSession)
    //
    //      // This is a non-streaming file based datasource.
    //      case (format: FileFormat, _) =>
    //        val allPaths = caseInsensitiveOptions.get("path") ++ paths
    //        val hadoopConf = sparkSession.sessionState.newHadoopConf()
    //        val globbedPaths = allPaths.flatMap { path =>
    //          val hdfsPath = new Path(path)
    //          val fs = hdfsPath.getFileSystem(hadoopConf)
    //          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    //          val globPath = SparkHadoopUtil.get.globPathIfNecessary(qualified)
    //
    //          if (globPath.isEmpty) {
    //            throw new AnalysisException(s"Path does not exist: $qualified")
    //          }
    //          // Sufficient to check head of the globPath seq for non-glob scenario
    //          // Don't need to check once again if files exist in streaming mode
    //          if (checkFilesExist && !fs.exists(globPath.head)) {
    //            throw new AnalysisException(s"Path does not exist: ${globPath.head}")
    //          }
    //          globPath
    //        }.toArray
    //
    //        val (dataSchema, partitionSchema) = getOrInferFileFormatSchema(format)
    //
    //        val fileCatalog = if (sparkSession.sqlContext.conf.manageFilesourcePartitions &&
    //          catalogTable.isDefined && catalogTable.get.tracksPartitionsInCatalog) {
    //          val defaultTableSize = sparkSession.sessionState.conf.defaultSizeInBytes
    //          new CatalogFileIndex(
    //            sparkSession,
    //            catalogTable.get,
    //            catalogTable.get.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
    //        } else {
    //          new InMemoryFileIndex(sparkSession, globbedPaths, options, Some(partitionSchema))
    //        }

    //        HadoopFsRelation(
    //          fileCatalog,
    //          partitionSchema = partitionSchema,
    //          dataSchema = dataSchema.asNullable,
    //          bucketSpec = bucketSpec,
    //          format,
    //          caseInsensitiveOptions)(sparkSession)
    //
    //      case _ =>
    //        throw new AnalysisException(
    //          s"$className is not a valid Spark SQL Data Source.")
    //    }

    null
  }

  /**
    * Get the schema of the given FileFormat, if provided by `userSpecifiedSchema`, or try to infer
    * it. In the read path, only managed tables by Hive provide the partition columns properly when
    * initializing this class. All other file based data sources will try to infer the partitioning,
    * and then cast the inferred types to user specified dataTypes if the partition columns exist
    * inside `userSpecifiedSchema`, otherwise we can hit data corruption bugs like SPARK-18510.
    * This method will try to skip file scanning whether `userSpecifiedSchema` and
    * `partitionColumns` are provided. Here are some code paths that use this method:
    *   1. `spark.read` (no schema): Most amount of work. Infer both schema and partitioning columns
    *   2. `spark.read.schema(userSpecifiedSchema)`: Parse partitioning columns, cast them to the
    * dataTypes provided in `userSpecifiedSchema` if they exist or fallback to inferred
    * dataType if they don't.
    *   3. `spark.readStream.schema(userSpecifiedSchema)`: For streaming use cases, users have to
    * provide the schema. Here, we also perform partition inference like 2, and try to use
    * dataTypes in `userSpecifiedSchema`. All subsequent triggers for this stream will re-use
    * this information, therefore calls to this method should be very cheap, i.e. there won't
    * be any further inference in any triggers.
    *
    * @param format the file format object for this DataSource
    * @return A pair of the data schema (excluding partition columns) and the schema of the partition
    *         columns.
    */
  private def getOrInferFileFormatSchema(format: FileFormat): (StructType, StructType) = {
    // the operations below are expensive therefore try not to do them if we don't need to, e.g.,
    // in streaming mode, we have already inferred and registered partition columns, we will
    // never have to materialize the lazy val below
    //    lazy val tempFileIndex = {
    //      val allPaths = caseInsensitiveOptions.get("path") ++ paths
    //      val hadoopConf = sparkSession.sessionState.newHadoopConf()
    //      val globbedPaths = allPaths.toSeq.flatMap { path =>
    //        val hdfsPath = new Path(path)
    //        val fs = hdfsPath.getFileSystem(hadoopConf)
    //        val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    //        SparkHadoopUtil.get.globPathIfNecessary(qualified)
    //      }.toArray
    //      new InMemoryFileIndex(sparkSession, globbedPaths, options, None)
    //    }
    //    val partitionSchema = if (partitionColumns.isEmpty) {
    //      // Try to infer partitioning, because no DataSource in the read path provides the partitioning
    //      // columns properly unless it is a Hive DataSource
    //      val resolved = tempFileIndex.partitionSchema.map { partitionField =>
    //        val equality = sparkSession.sessionState.conf.resolver
    //        // SPARK-18510: try to get schema from userSpecifiedSchema, otherwise fallback to inferred
    //        userSpecifiedSchema.flatMap(_.find(f => equality(f.name, partitionField.name))).getOrElse(
    //          partitionField)
    //      }
    //      StructType(resolved)
    //    } else {
    //      // maintain old behavior before SPARK-18510. If userSpecifiedSchema is empty used inferred
    //      // partitioning
    //      if (userSpecifiedSchema.isEmpty) {
    //        val inferredPartitions = tempFileIndex.partitionSchema
    //        inferredPartitions
    //      } else {
    //        val partitionFields = partitionColumns.map { partitionColumn =>
    //          val equality = sparkSession.sessionState.conf.resolver
    //          userSpecifiedSchema.flatMap(_.find(c => equality(c.name, partitionColumn))).orElse {
    //            val inferredPartitions = tempFileIndex.partitionSchema
    //            val inferredOpt = inferredPartitions.find(p => equality(p.name, partitionColumn))
    //            if (inferredOpt.isDefined) {
    //              logDebug(
    //                s"""Type of partition column: $partitionColumn not found in specified schema
    //                   |for $format.
    //                   |User Specified Schema
    //                   |=====================
    //                   |${userSpecifiedSchema.orNull}
    //                   |
    //                   |Falling back to inferred dataType if it exists.
    //                 """.stripMargin)
    //            }
    //            inferredOpt
    //          }.getOrElse {
    //            throw new AnalysisException(s"Failed to resolve the schema for $format for " +
    //              s"the partition column: $partitionColumn. It must be specified manually.")
    //          }
    //        }
    //        StructType(partitionFields)
    //      }
    //    }
    //
    //    val dataSchema = userSpecifiedSchema.map { schema =>
    //      val equality = sparkSession.sessionState.conf.resolver
    //      StructType(schema.filterNot(f => partitionSchema.exists(p => equality(p.name, f.name))))
    //    }.orElse {
    //      format.inferSchema(
    //        sparkSession,
    //        caseInsensitiveOptions,
    //        tempFileIndex.allFiles())
    //    }.getOrElse {
    //      throw new AnalysisException(
    //        s"Unable to infer schema for $format. It must be specified manually.")
    //    }
    //    (dataSchema, partitionSchema)
    null
  }

  /**
    * Returns true if there is a single path that has a metadata log indicating which files should
    * be read.
    */
  def hasMetadata(path: Seq[String]): Boolean = {
    path match {
//      case Seq(singlePath) =>
      //        try {
      //          val hdfsPath = new Path(singlePath)
      //          val fs = hdfsPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      //          val metadataPath = new Path(hdfsPath, FileStreamSink.metadataDir)
      //          val res = fs.exists(metadataPath)
      //          res
      //        } catch {
      //          case NonFatal(e) =>
      //            logWarning(s"Error while looking for metadata directory.")
      //            false
      //        }
      case _ => false
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
}

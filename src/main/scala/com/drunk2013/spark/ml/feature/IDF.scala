package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.util.SchemaUtils
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD


/**
  * Created by shuangfu on 17-2-7.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */

private[feature] trait IDFBase extends Params with HasInputCol with HasOutputCol {

  final val minDocFreq = new IntParam(this,
    "minDocFreq", "minimum number of documents in which a term should appear for filtering" +
      "(>=0)", ParamValidators.gtEq(0))

  setDefault(minDocFreq -> 0)

  def getMinDocFreq: Int = $(minDocFreq)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(inputCol), new VectorUDT)
  }
}

final class IDF(override val uid: String) extends Estimator[IDFModel] with IDFBase with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("idf"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setMinDocFreq(value: Int): this.type = set(minDocFreq, value)

  override def fit(dataset: Dataset[_]): IDFModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[OldVector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: Vector) => OldVectors.fromML(v)
    }

    val idf = new IDFLIB($(minDocFreq)).fit(input)
    copyValues(new IDFModel(uid, idf).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): IDF = defaultCopy(extra)
}

class IDFModel private[ml](override val uid: String, idfModel: IDFModelLIB) extends Model[IDFModel] with IDFBase with MLWritable {

  import IDFModel._

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val idf = udf { vec: Vector => idfModel.transform(OldVectors.fromML(vec)).asML }
    dataset.withColumn($(outputCol), idf(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): IDFModel = {
    val copied = new IDFModel(uid, idfModel)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new IDFModelWriter(this)

}

object IDFModel extends MLReadable[IDFModel] {

  private[IDFModel] class IDFModelWriter(instance: IDFModel) extends MLWriter {

    private case class Data(idf: Vector)

    override protected def saveImpl(path: String): Unit = {
    }
  }

  private class IDFModelReader extends MLReader[IDFModel] {

    private val className = classOf[IDFModel].getName

    override def load(path: String): IDFModel = {
      null
    }
  }

  override def read: MLReader[IDFModel] = new IDFModelReader

  override def load(path: String): IDFModel = super.load(path)
}
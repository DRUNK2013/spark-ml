package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.util.SchemaUtils
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.{IntParam, ParamValidators, Params}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
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
      case Row(v:Vector) => OldVectors.fromML(v)
    }

  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


}

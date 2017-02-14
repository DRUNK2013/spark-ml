package com.drunk2013.spark.util

import org.apache.spark.ml.param.Params
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
  * Created by shuangfu on 17-2-14.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
private[spark] trait PredictorParams extends Params with HasLabelCol with HasFeaturesCol with HasPredictionCol {
  /**
    * Validates and transforms the input schema with the provided param map.
    *
    * @param schema            input schema
    * @param fitting           whether this is in fitting
    * @param featuresDataType  SQL DataType for FeaturesType.
    *                          E.g., `VectorUDT` for vector features.
    * @return output schema
    */
  protected def validateAndTransformSchema(
                                            schema: StructType,
                                            fitting: Boolean,
                                            featuresDataType: DataType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    SchemaUtils.checkColumnType(schema, $(featuresCol), featuresDataType)
    if (fitting) {
      SchemaUtils.checkNumericType(schema, $(labelCol))

      this match {
        case p: HasWeightCol =>
          if (isDefined(p.weightCol) && $(p.weightCol).nonEmpty) {
            SchemaUtils.checkNumericType(schema, $(p.weightCol))
          }
        case _ =>
      }
    }
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}


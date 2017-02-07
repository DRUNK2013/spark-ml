package com.drunk2013.spark.ml.feature

import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

/**
  * Created by shuangfu on 17-2-6.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */

/**
  * Trait for shared param inputCol.
  */
private[ml] trait HasInputCol extends Params {

  /**
    * Param for input column name.
    *
    * @group param
    */
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  /** @group getParam */
  final def getInputCol: String = $(inputCol)
}

/**
  * Trait for shared param inputCols.
  */
private[ml] trait HasInputCols extends Params {

  /**
    * Param for input column names.
    *
    * @group param
    */
  final val inputCols: StringArrayParam = new StringArrayParam(this, "inputCols", "input column names")

  /** @group getParam */
  final def getInputCols: Array[String] = $(inputCols)
}

/**
  * Trait for shared param outputCol (default: uid + "__output").
  **/
private[ml] trait HasOutputCol extends Params {

  /**
    * Param for output column name.
    *
    * @group param
    */
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  /** @group getParam */
  final def getOutputCol: String = $(outputCol)
}


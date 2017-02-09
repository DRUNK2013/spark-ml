package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.InfoHelp
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

/**
  * Created by shuangfu on 17-1-20.
  */

class IDFSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  /**
    * tf-idf计算.=tf * idf
    *
    * @param dataset
    * @param model
    * @return
    */
  def scaleDataWithIDF(dataset: Array[Vector], model: Vector): Array[Vector] = {
    dataset.map {
      case data: DenseVector => {
        val res = data.toArray.zip(model.toArray).map { case (x, y) => x * y }
        Vectors.dense(res)
      }
      case data: SparseVector => {
        val res = data.indices.zip(data.values).map { case (id, value) =>
          (id, (value * model(id)))
        }
        Vectors.sparse(data.size, res)
      }

    }
  }

  test("params参数测试") {
    ParamsSuite.checkParams(new IDF)
    val model = new IDFModel("idf", new IDFModelLIB(org.apache.spark.mllib.linalg.Vectors.dense(1.0)))
    ParamsSuite.checkParams(model)
  }

  test("compute IDF with default parameter") {
    val numOfFeatures = 4
    val data = Array(
      Vectors.sparse(numOfFeatures, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeatures, Array(1), Array(1.0))
    )
    InfoHelp.show("data(as hashingTF data)", data)

    val numOfData = data.size

    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((numOfData + 1.0) / (x + 1.0))
    })
    InfoHelp.show("idf-----公式:log(DF(t,D)/TF(t,d))", idf)

    val expected = scaleDataWithIDF(data, idf)
    InfoHelp.show("expected result:TF-IDF", expected)

    val df = data.zip(expected).toSeq.toDF("features", "expected")
    InfoHelp.show("IDF-df", df)

    //val idfModel = new org.apache.spark.ml.feature.IDF()
    val idfModel = new IDF()
      .setInputCol("features")
      .setOutputCol("idfValue")
      .fit(df)

    InfoHelp.show("expected", idfModel.transform(df).select("expected"))
    InfoHelp.show("idfModel", idfModel.transform(df).select("idfValue"))
    idfModel.transform(df).select("idfValue", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x == y, "transformed vector is diffrent with expected vector")
    }

  }

  test("compute IDF with setter") {
    val numOfFeature = 4
    val data = Array(
      Vectors.sparse(numOfFeature, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(numOfFeature, Array(1), Array(1.0))
    )

    val numOfData = data.size
    val idf = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) math.log((numOfData + 1.0) / (x + 1.0)) else 0
    })

    val expected = scaleDataWithIDF(data, idf)

    val df = data.zip(expected).toSeq.toDF("features", "expected")

    //    val idfModel = new org.apache.spark.ml.feature.IDF()
    val idfModel = new IDF()
      .setInputCol("features")
      .setOutputCol("idfValue")
      .setMinDocFreq(1)
      .fit(df)

    idfModel.transform(df).select("idfValue", "expected").collect().foreach {
      case Row(x: Vector, y: Vector) =>
        assert(x == y, "Transformed vector is diffrent with expected vector")
    }

  }
}

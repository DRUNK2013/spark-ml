package com.drunk2013.spark.ml.feature

import com.drunk2013.spark.util.SchemaUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}


/**
  * Created by shuangfu on 17-2-6.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
/**
  * HashingTF 是一个Transformer，在文本处理中，接收词条的集合然后把这些集合转化成固定长度的特征向量。这个算法在哈希的同时会统计各个词条的词频。
  * ​ “词频－逆向文件频率”（TF-IDF）是一种在文本挖掘中广泛使用的特征向量化方法，它可以体现一个文档中词语在语料库中的重要程度。
  * ​ 词语由t表示，文档由d表示，语料库由D表示。词频TF(t,d)是词语t在文档d中出现的次数。文件频率DF(t,D)是包含词语的文档的个数。
  * 如果我们只使用词频来衡量重要性，很容易过度强调在文档中经常出现，却没有太多实际信息的词语，比如“a”，“the”以及“of”。
  * 如果一个词语经常出现在语料库中，意味着它并不能很好的对文档进行区分。TF-IDF就是在数值化文档信息，衡量词语能提供多少信息以区分文档。其定义如下：
  * IDF(t,D)=log|D|+1DF(t,D)+1IDF(t,D)=log|D|+1DF(t,D)+1
  * ​    *  此处 |D||D| 是语料库中总的文档数。公式中使用log函数，当词出现在所有文档中时，它的IDF值变为0。加1是为了避免分母为0的情况。TF-IDF 度量值表示如下：
  * TFIDF(t,d,D)=TF(t,d)⋅IDF(t,D)TFIDF(t,d,D)=TF(t,d)⋅IDF(t,D)
  * ​ 在Spark ML库中，TF-IDF被分成两部分：TF (+hashing) 和 IDF。
  *
  * @param uid
  */
class HashingTF(override val uid: String) extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("hashingTF"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)", ParamValidators.gt(0))

  /**
    * 设置二元太模式,即:词频大于0次的,都是这为1
    */
  val binary = new BooleanParam(this, "binary", "If true, all non zero counts are set to 1, " +
    "This is useful for discrete probabilistic models that model binary events rather than integer counts.")

  setDefault(numFeatures -> (1 << 18), binary -> false)

  def getNumFeatures: Int = $(numFeatures)

  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  def getBinary: Boolean = $(binary)

  def setBinary(value: Boolean): this.type = set(binary, value)

  /**
    * 计算每个文档(这里是每条句子)中每个词对词频,并给每个词添加一个哈希值,最后返回向量
    *
    * @param dataset
    * @return
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.printSchema()
    val outputSchema = transformSchema(dataset.schema)
    val hashingTF = new HashingTFLIB($(numFeatures)).setBinary($(binary))
    // TODO: Make the hashingTF.transform natively in ml framework to avoid extra conversion.
    val t = udf { terms: Seq[_] => hashingTF.transform(terms).asML }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    println(schema)
    print(inputType)
    require(inputType.isInstanceOf[ArrayType], s"输入的字段类型,必须是ArrayType.类型 $inputType 非法")

    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField)
  }

  override def copy(extra: ParamMap): HashingTF = defaultCopy(extra)

}

object HashingTF extends DefaultParamsReadable[HashingTF] {
  override def load(path: String): HashingTF = super.load(path)
}

package com.drunk2013.spark.sql

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}

/**
  * Created by shuangfu on 17-4-21.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  * DataSet 常用function练习
  *
  *
  * DataSet是一个强类型的带有数据结构对象的数据集合。可以进行并行的进行transformed，或者进行关联操作。
  * DataFrame是Dataset of [Row] 的特殊一种，是类型不安全，数据处理、存储是在宿主机的JVM中。
  *
  * DataSet提供了transformation和actions操作。
  * transformation：一个DataSet转换到另外一个DataSet。
  * actions：触发计算然后返回结果（比如数据聚合到drive端或者数据写入到文件等）。
  * 例如：map,filter,select和aggregate(`groupBy`)是transformation常用的操作。
  * count，show，写数据到文件系统，是常用的actions操作。
  *
  * Datasets 是中惰操作，仅当actions操作触发，才进行计算。
  * Datasets 会呈现一个逻辑执行计划，以便描述其计算和处理数据的过程。
  * 可以使用explain函数，查看详细的物理和逻辑计划过程。
  *
  * Datasets这种带有强类型的特殊数据结构，是非常高效的，其包含一个类型解码器（Encoder）。
  * 以把spark core中的内部计算过程中会携带译码器。
  *
  *
  *
  */

class Dataset2[T] private[sql](
                               val sparkSession: SparkSession,
                               val queryExecution: QueryExecution,
                               encoder: Encoder[T])
  extends Serializable {

  /**
    * :: Experimental ::
    * Joins this Dataset returning a `Tuple2` for each pair where `condition` evaluates to
    * true.
    *
    * This is similar to the relation `join` function with one important difference in the
    * result schema. Since `joinWith` preserves objects present on either side of the join, the
    * result schema is similarly nested into a tuple under the column names `_1` and `_2`.
    *
    * This type of join can be useful both for preserving type-safety with the original object
    * types as well as working with relational data where either side of the join has column
    * names in common.
    *
    * //    * @param other     Right side of the join.
    * //    * @param condition Join expression.
    * //    * @param joinType  Type of join to perform. Default `inner`. Must be one of:
    * `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
    * `right`, `right_outer`, `left_semi`, `left_anti`.
    *
    * @group typedrel
    * @since 1.6.0
    */

  //  def joinWith[U](other: Dataset[U], condition: Column, joinType: String): Dataset[(T, U)] = {
  // Creates a Join node and resolve it first, to get join condition resolved, self-join resolved,
  // etc.
  //    val joined = sparkSession.sessionState.executePlan(
  //      Join(
  //        this.logicalPlan,
  //        other.logicalPlan,
  //        JoinType(joinType),
  //        Some(condition.expr))).analyzed.asInstanceOf[Join]
  //
  //    // For both join side, combine all outputs into a single column and alias it with "_1" or "_2",
  //    // to match the schema for the encoder of the join result.
  //    // Note that we do this before joining them, to enable the join operator to return null for one
  //    // side, in cases like outer-join.
  //    val left = {
  //      val combined = if (this.exprEnc.flat) {
  //        assert(joined.left.output.length == 1)
  //        Alias(joined.left.output.head, "_1")()
  //      } else {
  //        Alias(CreateStruct(joined.left.output), "_1")()
  //      }
  //      Project(combined :: Nil, joined.left)
  //    }
  //
  //    val right = {
  //      val combined = if (other.exprEnc.flat) {
  //        assert(joined.right.output.length == 1)
  //        Alias(joined.right.output.head, "_2")()
  //      } else {
  //        Alias(CreateStruct(joined.right.output), "_2")()
  //      }
  //      Project(combined :: Nil, joined.right)
  //    }
  //
  //    // Rewrites the join condition to make the attribute point to correct column/field, after we
  //    // combine the outputs of each join side.
  //    val conditionExpr = joined.condition.get transformUp {
  //      case a: Attribute if joined.left.outputSet.contains(a) =>
  //        if (this.exprEnc.flat) {
  //          left.output.head
  //        } else {
  //          val index = joined.left.output.indexWhere(_.exprId == a.exprId)
  //          GetStructField(left.output.head, index)
  //        }
  //      case a: Attribute if joined.right.outputSet.contains(a) =>
  //        if (other.exprEnc.flat) {
  //          right.output.head
  //        } else {
  //          val index = joined.right.output.indexWhere(_.exprId == a.exprId)
  //          GetStructField(right.output.head, index)
  //        }
  //    }
  //
  //    implicit val tuple2Encoder: Encoder[(T, U)] =
  //      ExpressionEncoder.tuple(this.exprEnc, other.exprEnc)
  //
  //    withTypedPlan(Join(left, right, joined.joinType, Some(conditionExpr)))
  //  }
}

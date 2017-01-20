package com.github2013.spark.ml.fpg
/**
  * Created by shuangfu on 17-1-20.
  */
class FPGrowth {


}

object FPGrowth {

  /*
  FreqItemset 结构体,Item为泛型参数
   */
  class FreqItemset[Item](
                           val items: Array[Item],
                           val freq: Long
                         ) extends Serializable {

    override def toString: String = {
      s"${items.mkString("{", ",", "}")}:$freq"
    }

  }

}

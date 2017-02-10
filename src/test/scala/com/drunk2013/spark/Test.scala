package com.drunk2013.spark

import scala.collection.mutable

/**
  * Created by shuangfu on 17-1-23.
  */

class Persion[T](val n: T) {
  var name: T = n
  var count: Int = 0

  override def toString: String = s"name:${name},count:${count}"
}

object Test {
  def main(args: Array[String]): Unit = {
    println("开始测试.....")
    //    val root = new Persion("zsf")
    //    println(root)
    //    val curr = root
    //    println(curr)
    //    curr.count = 100
    //    println(curr)
    //    println(root)

    val map = new mutable.HashMap[String, Array[Int]]().empty
    map.put("a", Array(1))
    map.put("b", Array(2))
    map.put("c", Array(3))
    map.put("d", Array(4))

    println(map.keys.zipWithIndex.toMap)
  }
}


package com.drunk2013.spark

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
      val root = new Persion("zsf")
      println(root)
      val curr = root
      println(curr)
      curr.count=100
      println(curr)
      println(root)
    }
  }


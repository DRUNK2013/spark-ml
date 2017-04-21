package com.drunk2013.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
  * Created by shuangfu on 17-3-13.
  * Author : DRUNK
  * email :len1988.zhang@gmail.com
  */
class SentenceSimilarity extends SparkFunSuite {
  test("计算句子的相似度") {

    val str1 = "好 0.308095 0.260854 0.218423 -0.163816 -0.081939 -0.300082 -0.446240 0.091916 -0.039553 0.268441 -0.078946 -0.148087 0.048889 -0.185016 0.075901 0.341787 -0.446655 -0.197572 0.400719 -0.012118 -0.036079 -0.333380 0.091312 -0.180486 0.044591 -0.000094 0.005762 -0.080121 -0.064560 0.106166 0.158442 0.029040 -0.214843 -0.132369 0.107824 -0.007312 -0.171883 -0.082665 0.233375 0.214032 -0.092535 -0.136843 -0.030200 0.349872 -0.248416 0.133739 0.092000 0.055878 0.000973 -0.161080 -0.002635 -0.321363 0.157583 -0.359215 -0.028509 0.023144 -0.081449 -0.169485 -0.125784 -0.199522 0.298608 -0.268735 0.185146 -0.216350 -0.268447 0.233605 0.121831 -0.194977 0.156172 0.249198 0.213381 0.262904 0.047823 -0.347702 -0.127381 0.066814 -0.751850 -0.155618 -0.618490 -0.391818 -0.126877 -0.184392 0.094755 -0.148505 0.184434 0.229090 -0.090922 -0.107319 0.220740 -0.171391 0.045824 0.392089 -0.165795 -0.068526 0.098520 0.378222 -0.284254 0.570085 0.481368 -0.152356"
    val name1 = str1.split(" ")(0)
    val v1 = Vectors.dense(str1.split(" ").drop(1).map(_.toDouble))
    println(s"[$name1] vector:$v1")
    val str2 = "想 0.418329 0.190108 0.112397 0.079051 -0.173652 -0.054699 -0.523232 0.355476 -0.312209 0.210850 -0.291136 0.067267 -0.049935 -0.118880 0.093084 0.282199 -0.462725 -0.045301 0.208507 0.059309 0.016221 -0.365295 0.269508 -0.451481 -0.135601 -0.085419 -0.096142 0.082197 -0.330584 0.253212 0.503225 -0.127691 -0.196430 -0.103057 0.055180 0.079676 -0.177475 -0.246307 0.333580 0.002781 -0.005291 -0.022608 -0.050252 0.160665 -0.083227 0.156518 0.048410 0.099742 -0.053633 -0.414252 -0.111687 -0.111830 0.188955 -0.355671 -0.371397 0.065974 -0.267373 -0.038254 0.064041 -0.108897 0.072989 -0.037082 0.077775 -0.220709 -0.554355 0.317554 -0.032246 -0.294871 0.204487 0.257546 0.258468 0.288912 -0.290064 -0.349973 -0.174462 0.087854 -0.603982 -0.120257 -0.456913 -0.488832 -0.259217 -0.248309 -0.116841 0.022964 0.217261 0.049742 -0.060882 0.063246 0.098483 0.068146 0.029348 0.331052 -0.192864 0.137344 0.130971 0.423699 -0.331796 0.508660 0.330009 -0.214401"
    //    val str2 = "漂亮 0.119805 -0.488018 -0.045659 -0.586229 0.042617 -0.370186 -0.359278 0.336750 0.111584 0.958039 -0.008384 0.537372 0.003234 0.148861 -0.814669 0.038588 -0.165824 0.306883 -0.230930 -0.070835 0.118015 -0.523662 0.650473 -0.539890 -0.778166 0.131354 -0.446081 0.151529 -0.487034 0.480590 0.754703 -1.115257 -0.816481 0.628962 0.089738 0.172652 0.575114 -0.992819 -0.050394 -0.391968 -1.233873 -0.352877 0.170589 0.220902 -0.220584 -0.221832 -0.222937 -0.507669 -1.016831 -0.300637 -0.282095 -0.758638 -0.031032 0.116429 0.577985 -1.356924 0.658104 0.591413 -0.352420 -0.993911 -0.075833 -0.274236 -0.182454 -1.191142 -0.424546 -0.076803 0.082809 0.243750 0.170159 0.987280 1.459083 0.437873 -0.023951 0.217280 0.507000 -0.102520 -0.671712 -0.484736 -0.821172 0.100927 -0.031931 -0.349401 0.448617 0.423937 0.631220 0.577786 -0.205214 0.267976 1.192998 -0.105701 0.054365 0.475658 0.142781 0.211301 -0.247790 0.447296 0.229011 1.339919 -0.266052 -0.244175"
    val name2 = str2.split(" ")(0)
    val v2 = Vectors.dense(str2.split(" ").drop(1).map(_.toDouble))
    println(s"[$name2] vector:$v2")

    val distance = Vectors.sqdist(v1, v2)
    println(s"[$name1]--[$name2],distance:" + distance)

    val cosine = getCosine(v1, v2)
    println(s"[$name1]--[$name2],cosine:" + cosine)


//    println(s"[$name1]--[$name2],cosine O:" + math.sin(30.0d))
  }


  def getCosine(v1: Vector, v2: Vector): Double = {
    require(v1.size == v2.size, s"Two vector size must be equal," +
      s"but get v1 size:${v1.size},v2 size:${v2.size}")
    var squXSum = 0d
    //x*x sum
    var squYSum = 0d
    //y*y sum
    var xMultiYSum = 0d
    for (i <- 0 until v1.size) {
      xMultiYSum += v1.apply(i) * v2.apply(i)
      squXSum += v1.apply(i) * v1.apply(i)
      squYSum += v2.apply(i) * v2.apply(i)
    }

    xMultiYSum / (math.sqrt(squXSum) * math.sqrt(squYSum))
  }

}
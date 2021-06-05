package com.yunkeji.spark

object TestURL {
  def main(args: Array[String]): Unit = {
    val url = "http://javaee.51doit.cn/xiaoxu"
    val arr: Array[String] = url.split("/")

    println(arr(2))
    println(arr(3))
    val arrUrl: Array[String] = arr(2).split("\\.")
    println(arrUrl(0))

    /**
     * javaee.51doit.cn
     * xiaoxu
     * javaee
     */

    /**
     * 1、String.split()
     *
     * String有个方法是分割字符串  .split()。但是有写字符串是需要转义才能分割，不然就会出错。
     *
     * 需要转义的字符串：.  $  |   (   )  [   {   ^  ?  *  +  \\      共12个特殊字符，遇到以这些字符进行分割字符串的时候，需要在这些特殊字符前加双反斜杠 \\
     *
     * 例如：
     *
     * str.split("\\.")
     *
     * str.split("\\$")
     *
     * str.split("\\|")
     *
     * str.split("\\(")
     *
     * str.split("\\)")
     *
     * str.split("\\[")
     *
     * str.split("\\{")
     *
     * str.split("\\^")
     *
     * str.split("\\?")
     *
     * str.split("\\*")
     *
     * str.split("\\+")
     *
     * str.split("\\\\")
     */


  }
}

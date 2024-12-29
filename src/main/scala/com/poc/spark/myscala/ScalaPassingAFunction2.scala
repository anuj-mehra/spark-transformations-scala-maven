package com.poc.spark.myscala

object ScalaPassingAFunction2 extends App {

  def add(a: Int, b: Int): Int = {
    a + b
  }

  def doSum(num1: Int, num2: Int, f: (Int, Int) => Int) = {
    f(num1, num2)
  }

  val response = doSum(5, 5, add)
  println(response)

}

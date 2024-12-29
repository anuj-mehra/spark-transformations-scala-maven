package com.poc.spark.myscala

object ScalaPassingAFunction extends App {

  val result = findSum(10, 20, addNumbers)
  println(result)

  def addNumbers(num1: Int, num2: Int): Int = {
    num1 + num2
  }

  def findSum(num1: Int, num2: Int, func: (Int, Int) => Int): Int = {
    func(num1, num2)
  }
}

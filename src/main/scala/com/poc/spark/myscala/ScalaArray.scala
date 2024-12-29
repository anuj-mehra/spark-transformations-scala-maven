package com.poc.spark.myscala

object ScalaArray extends App{

  val arr = new Array[String](5)
  arr(0) = "a"
  arr(1) = "b"
  arr(2) = "c"
  arr(3) = "d"
  arr(4) = "e"

  arr.foreach(e => println(e))
}

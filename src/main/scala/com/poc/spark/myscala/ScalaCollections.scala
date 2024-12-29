package com.poc.spark.myscala

import scala.collection.mutable

object ScalaCollections extends App{

  //val myimmutableList = new MyList
  //myimmutableList.immutableList
  //myimmutableList.mutableList

  /*val myset = new MySet
  myset.mutable*/

  val mymap = new MyMap
  mymap.mutable
}


class MySet {

  def immutable : Unit = {
    val myset = Set("1", "2", "3")
    myset.foreach(e => println(e))
  }

  def mutable: Unit = {
    val myset = scala.collection.mutable.Set.empty[String]
    myset += "a"
    myset += "b"

    println(myset.contains("a"))

    myset.toList.foreach(e => println(e))

  }

}

class MyMap {

  def immutable: Unit = {
    val mymap = Map("1" -> "a", "2" -> "b")

    mymap.keys.foreach(e => println(mymap.get(e).get))

    println(mymap.get("5"))
  }

  def mutable: Unit = {

    val mymap = scala.collection.mutable.Map.empty[String, String]
    mymap += ("1" -> "a")
    mymap += ("2" -> "b")

    println(mymap.get("1"))

    mymap.foreach((k) => println(mymap.get(k._1)))

    mymap -= "2"
  }
}

class MyList {

  def immutableList: Unit = {
    val immutableList: List[Int] = List(1,2,3,4)
    val emptyList: List[Int] = List.empty[Int]

    immutableList.foreach(e => println(e))
  }

  def mutableList: Unit = {

    val lb = mutable.ListBuffer[Int]()
    lb += 1
    lb += 2
    lb.append(3)
    lb.prepend(0)

    lb.foreach(e => println(e))
  }
}


package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig

object AddColumnUsingUdf extends App with Serializable{

  val spark = SparkSessionConfig("AddColumnUsingUdf", true).getSparkSession

  //val inputdata = List()

  val string = null
  val toBeTested = Option(string)

  toBeTested.isDefined match {
    case true =>
      println("--not null--")
    case false =>
      println("--null--")
  }

}

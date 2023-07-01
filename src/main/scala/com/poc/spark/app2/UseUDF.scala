package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object UseUDF extends App{
  val sparkSession: SparkSession = SparkSessionConfig("UseUDF", true).getSparkSession

  val obj = new UseUDF
  obj example sparkSession
}

class UseUDF {

  val example:(SparkSession) => Unit = (sparkSession: SparkSession) => {

  }

}

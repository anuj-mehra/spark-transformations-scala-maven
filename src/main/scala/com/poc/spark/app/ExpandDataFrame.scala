package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig

object ExpandDataFrame extends App {

  val sparkSession = SparkSessionConfig("ExpandDataFrame", true).getSparkSession

  val processor  = new ExpandDataFrame

}
class ExpandDataFrame {

}

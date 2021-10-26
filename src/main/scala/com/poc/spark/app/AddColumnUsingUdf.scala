package com.poc.spark.app

import com.poc.spark.config.SparkSessionConfig

class AddColumnUsingUdf extends App with Serializable{

  val spark = SparkSessionConfig("AddColumnUsingUdf", true).getSparkSession

  //val inputdata = List()

}

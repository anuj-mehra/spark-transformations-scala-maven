package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object WhenWhereOtherwise extends App with Serializable{

  implicit val sparkSession: SparkSession = SparkSessionConfig("WhenWhereOtherwise", true).getSparkSession



}

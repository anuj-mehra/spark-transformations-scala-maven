package com.poc.spark.app2

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object GroupByExamples extends App with Serializable{

  implicit val sparkSession: SparkSession = SparkSessionConfig("GroupByExamples", true).getSparkSession

}

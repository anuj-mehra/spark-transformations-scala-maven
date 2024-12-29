package com.poc.spark.app5

import com.poc.spark.app4.WordCount
import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.SparkSession

object ReadFileAndSavePartitionedParquet extends App {


  implicit val spark: SparkSession = SparkSessionConfig("ReadFileAndSavePartitionedParquet", true).getSparkSession

  val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/csv-file-with-header-trailor.csv"

  val inputDf = spark.read.text()

}

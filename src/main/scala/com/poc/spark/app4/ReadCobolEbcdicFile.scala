package com.poc.spark.app4

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadCobolEbcdicFile extends App{

  implicit val sparkSession: SparkSession = SparkSessionConfig("ReadCsvFile", true).getSparkSession
  val obj = new ReadCsvFile
  val inputFilePath = ""
}

class ReadCobolEbcdicFile(implicit spark: SparkSession) extends Serializable  {

  def read(inputEbcdicFilePath: String, copybookFilePath: String): DataFrame = {

    spark.read.format("za.co.cobrix.spark.cobol.source")
      .option("copybook", copybookFilePath)
      .option("schema_retention_policy", "collapse_root")
      .option("generate_record_id", true)
      .option("string_trimming_policy", "none")
      .load(inputEbcdicFilePath)
  }
}

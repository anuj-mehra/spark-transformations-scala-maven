package com.poc.spark.app4

import com.poc.spark.config.SparkSessionConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadCsvFile extends App{

  implicit val sparkSession: SparkSession = SparkSessionConfig("ReadCsvFile", true).getSparkSession
  val obj = new ReadCsvFile
  val inputFilePath = "/Users/anujmehra/git/spark-transformations-scala-maven/src/main/resources/csv-file-1.csv"

 /* val df1 = obj.readCsvWithHeader(inputFilePath)
  df1.show(false)*/

  val df2 = obj.readCsvWithoutHeader(inputFilePath)
    .toDF(List("FirstName", "MiddleName","LastName","City"):_*)
  df2.show(false)
}

class ReadCsvFile(implicit spark: SparkSession) extends Serializable {

  def readCsvWithHeader(csvFilePath: String): DataFrame ={

    val options = Map("inferSchema"-> "true",
      "delimiter"->"|",
      "header"->"true")

    spark.read.options(options).csv(csvFilePath)
  }

  def readCsvWithoutHeader(csvFilePath: String): DataFrame = {
    val options = Map("inferSchema"-> "true",
      "delimiter"->"|",
      "header"->"false")
    spark.read.options(options).csv(csvFilePath)
  }
}
